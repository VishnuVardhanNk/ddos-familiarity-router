package ddos.stream

import ddos.common.{Postgres, Session}
import ddos.ingest.BuildFeatures
import ddos.learn.{Gate, Memory}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, RandomForestClassificationModel}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tree.{DecisionPath, Node}
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

import java.sql.Timestamp
import java.util.Properties
import scala.collection.JavaConverters._
import scala.collection.mutable

case class Routing(batch_id: Long, model: String, flows: Long, unfamiliar_flows: Long,
                   alone_tp: Long, alone_fn: Long, alone_fp: Long, alone_tn: Long,
                   routed_tp: Long, routed_fn: Long, routed_fp: Long, routed_tn: Long,
                   labels_asked: Long, forest_flows: Long, memory_flows: Long, traffic_time: Timestamp)

object LiveScorer {
  val models: Seq[String] =
    Seq("logistic_regression", "decision_tree", "random_forest", "gradient_boosted_trees", "neural_network")
  val LabelsPerTick = 24
  val AskShare = 0.25
  val MemoryLabels = 1000
  val ForestBusy = 0.01
  val Front = "decision_tree"
  val ReasonSample = 30
  val Evidence: Seq[String] = Seq("flow_pkts_s", "flow_byts_s", "pkt_len_mean", "flow_duration",
    "tot_fwd_pkts", "tot_bwd_pkts", "syn_flag_cnt", "ack_flag_cnt")

  val flowSchema: StructType = StructType(
    Seq(
      StructField("flow_id", StringType), StructField("ts", TimestampType),
      StructField("src_ip", StringType), StructField("src_port", LongType),
      StructField("dst_ip", StringType), StructField("dst_port", LongType),
      StructField("label_family", StringType), StructField("is_attack", IntegerType)
    ) ++ BuildFeatures.features.map(f => StructField(f, DoubleType)))

  def startingOffsets(fraction: Double): String = {
    if (fraction <= 0) return "earliest"
    val settings = new Properties()
    settings.put("bootstrap.servers", "kafka:9092")
    settings.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    settings.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](settings)
    val partition = new TopicPartition("flows", 0)
    val end = consumer.endOffsets(Seq(partition).asJava).get(partition).longValue()
    consumer.close()
    s"""{"flows":{"0":${(end * fraction).toLong}}}"""
  }

  def tally(rows: DataFrame, cases: Seq[(Column, Column)]): Seq[Array[Long]] = {
    val sums = for ((predicted, where) <- cases; (attack, answer) <- Seq((1, 1.0), (1, 0.0), (0, 1.0), (0, 0.0)))
      yield coalesce(sum(when(where && col("is_attack") === attack && predicted === answer, 1L).otherwise(0L)), lit(0L))
    val row = rows.agg(sums.head, sums.tail: _*).first()
    cases.indices.map(i => Array(row.getLong(4 * i), row.getLong(4 * i + 1), row.getLong(4 * i + 2), row.getLong(4 * i + 3)))
  }

  def main(args: Array[String]): Unit = {
    val spark = Session("LiveScorer", 6)
    import spark.implicits._
    val flowsPerBatch = args.headOption.getOrElse("12000")
    val startAt = args.lift(1).map(_.toDouble).getOrElse(0.0)

    val encoder = PipelineModel.load("/models/encoder")
    val gate = Gate.load(spark)
    val detectors = models.map(name => name -> PipelineModel.load(s"/models/whole/cross_day/detection/$name"))

    spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "label_requests").option("startingOffsets", "earliest").load()
      .selectExpr("CAST(value AS STRING) AS value")
      .writeStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("topic", "labels")
      .option("checkpointLocation", "/checkpoints/analyst")
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()

    val flows = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("subscribe", "flows")
      .option("startingOffsets", startingOffsets(startAt))
      .option("maxOffsetsPerTrigger", flowsPerBatch)
      .option("minPartitions", "6")
      .load()
      .select(from_json(col("value").cast("string"), flowSchema).as("flow"))
      .select("flow.*")

    val scored = detectors.foldLeft(gate.measure(encoder.transform(flows))) { case (df, (name, model)) =>
      model.transform(df).withColumn(name, col("prediction"))
        .drop("rawPrediction", "probability", "prediction", "scaled_features")
    }

    val treeRoots: Seq[Node] = Seq(detectors.toMap.apply("decision_tree").stages(0)
      .asInstanceOf[DecisionTreeClassificationModel].rootNode)
    val forestRoots: Seq[Node] = detectors.toMap.apply("random_forest").stages(0)
      .asInstanceOf[RandomForestClassificationModel].trees.map(_.rootNode).toSeq
    val featureNames = BuildFeatures.features
    val asked = mutable.Set[Long]()
    val pattern = xxhash64(vector_to_array(col("features")))
    val close = col("nn_distance") <= gate.limit

    val writeBatch: (DataFrame, Long) => Unit = (arrived, batchId) => {
      val started = System.nanoTime()
      val batch = arrived.localCheckpoint()

      val perModel = batch.select(
        col("ts"), date_trunc("minute", col("ts")).as("minute"), col("label_family"), col("is_attack"),
        explode(array(models.map(m => struct(lit(m).as("model"), col(m).as("predicted"))): _*)).as("p"))
      def countWhere(attack: Int, predicted: Double) =
        sum(when(col("is_attack") === attack && col("p.predicted") === predicted, 1L).otherwise(0L))

      val scores = perModel.groupBy(col("minute"), col("label_family"), col("p.model").as("model")).agg(
        count(lit(1)).as("flows"),
        countWhere(1, 1.0).as("attacks_caught"),
        countWhere(1, 0.0).as("attacks_missed"),
        countWhere(0, 1.0).as("false_alarms"),
        countWhere(0, 0.0).as("benign_passed"),
        max("ts").as("latest"))
      val scoreRows = scores.collect()
      val oneModel = scoreRows.filter(_.getAs[String]("model") == models.head)
      val flowCount = oneModel.map(_.getAs[Long]("flows")).sum
      val trafficTime = oneModel.map(_.getAs[Timestamp]("latest")).maxBy(_.getTime)

      Postgres.write(
        spark.createDataFrame(spark.sparkContext.parallelize(scoreRows.toSeq, 1), scores.schema).drop("latest")
          .withColumn("batch_id", lit(batchId))
          .withColumn("processed_at", current_timestamp()),
        "live_scores", "append")

      val anyModelWrong = models.map(m => col(m) =!= col("is_attack")).reduce(_ || _)
      Postgres.write(
        batch.filter(anyModelWrong)
          .select((Seq("ts", "src_ip", "src_port", "dst_ip", "dst_port", "label_family", "is_attack") ++ models).map(col): _*)
          .limit(40)
          .withColumn("batch_id", lit(batchId))
          .withColumn("processed_at", current_timestamp()),
        "live_mistakes", "append")

      val alone = tally(batch, models.flatMap(m => Seq((col(m), !col("unfamiliar")), (col(m), col("unfamiliar")))))
      val unfamiliarCount = alone(1).sum
      val located = Memory.nearest(spark, Memory.read(spark), batch.filter(col("unfamiliar"))).persist()
      val routed = tally(located, Seq((when(close, col("nn_label")).otherwise(col("random_forest")), lit(true)))).head
      val memoryFlows = located.filter(close).count()
      val forestFlows = unfamiliarCount - memoryFlows

      def howMany(where: Column) = sum(when(where, 1L).otherwise(0L))
      val measures = Seq(
        max("is_attack").as("is_attack"),
        howMany(col("unfamiliar")).as("unfamiliar"),
        howMany(!col("unfamiliar") && col(Front) === 1.0).as("flagged_familiar"),
        avg(when(col("protocol") === 6.0, 1.0).otherwise(0.0)).as("tcp_share")) ++ Evidence.map(f => avg(col(f)).as(f))
      val familiarSide = batch.groupBy("label_family").agg(count(lit(1)).as("flows"), measures: _*)
      val unfamiliarSide = located.groupBy("label_family").agg(
        howMany(close).as("from_memory"), howMany(!close).as("from_forest"),
        howMany(when(close, col("nn_label")).otherwise(col("random_forest")) === 1.0).as("flagged_unfamiliar"))
        .collect().map(r => r.getString(0) -> (r.getLong(1), r.getLong(2), r.getLong(3))).toMap
      val extra = udf((family: String) => unfamiliarSide.getOrElse(family, (0L, 0L, 0L)))
      Postgres.write(
        familiarSide.withColumn("far", extra(col("label_family")))
          .withColumn("from_memory", col("far._1")).withColumn("from_forest", col("far._2"))
          .withColumn("flagged", col("flagged_familiar") + col("far._3")).drop("far", "flagged_familiar")
          .withColumn("batch_id", lit(batchId)).withColumn("traffic_time", lit(trafficTime))
          .withColumn("processed_at", current_timestamp()),
        "live_traffic", "append")

      val explained = batch.sample(withReplacement = false, 0.02).select("label_family", "unfamiliar", "features").collect()
        .groupBy(r => (r.getString(0), r.getBoolean(1))).toSeq
        .flatMap { case ((family, unfamiliar), sampled) =>
          val rows = sampled.take(ReasonSample)
          val roots = if (unfamiliar) forestRoots else treeRoots
          val credit = new Array[Double](featureNames.size)
          for (row <- rows; root <- roots) {
            val part = DecisionPath.contributions(root, row.getAs[Vector](2), featureNames.size)
            featureNames.indices.foreach(i => credit(i) += part(i) / roots.size / rows.length)
          }
          featureNames.zip(credit).sortBy(pair => -math.abs(pair._2)).take(8).map { case (feature, value) =>
            (batchId, family, if (unfamiliar) "random forest" else "decision tree", feature, value, rows.length)
          }
        }
      Postgres.write(
        explained.toDF("batch_id", "label_family", "decided_by", "feature", "contribution", "connections_explained")
          .withColumn("processed_at", current_timestamp()),
        "live_reasons", "append")

      val candidates = located.filter(!close).select(pattern.as("pattern"), col("cluster")).distinct().collect()
        .map(r => (r.getLong(0), r.getInt(1))).filterNot(c => asked(c._1))
      val wanted =
        if (forestFlows <= ForestBusy * flowCount) 0
        else Seq(LabelsPerTick, MemoryLabels - asked.size, math.ceil(candidates.length * AskShare).toInt).min
      val chosen = candidates.groupBy(_._2).values.toSeq.flatMap(_.zipWithIndex).sortBy(_._2).take(wanted).map(_._1)
      if (chosen.nonEmpty) {
        located.withColumn("pattern", pattern).filter(col("pattern").isin(chosen.map(_._1): _*)).dropDuplicates("pattern")
          .select(to_json(struct(col("pattern"), col("cluster"), lit(batchId).as("batch_id"),
            vector_to_array(col("gate_scaled")).as("scaled"), col("is_attack").as("answer"))).as("value"))
          .write.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("topic", "label_requests").save()
        asked ++= chosen.map(_._1)
      }

      val routing = models.zipWithIndex.map { case (model, i) =>
        val near = alone(2 * i)
        val far = alone(2 * i + 1)
        Routing(batchId, model, flowCount, unfamiliarCount,
          near(0) + far(0), near(1) + far(1), near(2) + far(2), near(3) + far(3),
          near(0) + routed(0), near(1) + routed(1), near(2) + routed(2), near(3) + routed(3),
          asked.size, forestFlows, memoryFlows, trafficTime)
      }
      Postgres.write(routing.toDF().withColumn("processed_at", current_timestamp()), "live_routing", "append")

      located.unpersist()
      val seconds = (System.nanoTime() - started) / 1e9
      Postgres.write(
        Seq((batchId, flowCount, seconds, trafficTime)).toDF("batch_id", "flows", "seconds", "traffic_time")
          .withColumn("processed_at", current_timestamp()),
        "live_batches", "append")
    }

    scored.writeStream
      .foreachBatch(writeBatch)
      .option("checkpointLocation", "/checkpoints/live_scorer")
      .start()

    spark.streams.awaitAnyTermination()
  }
}
