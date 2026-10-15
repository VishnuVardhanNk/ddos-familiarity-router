package ddos.test

import ddos.common.Session
import ddos.learn.{Labelled, Memory, Gate => LiveGate}
import ddos.stream.LiveScorer
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, RandomForestClassificationModel}
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object ThroughputCheck {
  def main(args: Array[String]): Unit = {
    val spark = Session("ThroughputCheck", 6)
    val from = args.headOption.map(_.toDouble).getOrElse(0.70)
    val to = args.lift(1).map(_.toDouble).getOrElse(0.86)

    val encoder = PipelineModel.load("/models/encoder")
    val forest = PipelineModel.load("/models/whole/cross_day/detection/random_forest").stages(0)
      .asInstanceOf[RandomForestClassificationModel]
    val tree = PipelineModel.load("/models/whole/cross_day/detection/decision_tree").stages(0)
      .asInstanceOf[DecisionTreeClassificationModel]
    val forestAnswer = udf((v: Vector) => forest.predict(v))
    val treeAnswer = udf((v: Vector) => tree.predict(v))

    val flows = encoder.transform(
      spark.read.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", "flows")
        .option("startingOffsets", LiveScorer.startingOffsets(from)).option("endingOffsets", LiveScorer.startingOffsets(to)).load()
        .select(from_json(col("value").cast("string"), LiveScorer.flowSchema).as("flow")).select("flow.*"))
      .select("features", "is_attack").repartition(6).persist(StorageLevel.MEMORY_AND_DISK)
    val total = flows.count()

    def timed(rows: DataFrame, answer: Column, path: Column): (Double, Double, Double, Double) = {
      def cell(attack: Int, predicted: Double) = sum(when(col("is_attack") === attack && answer === predicted, 1L).otherwise(0L))
      def once() = {
        val started = System.nanoTime()
        val r = rows.agg(cell(1, 1.0), cell(1, 0.0), cell(0, 1.0), cell(0, 0.0),
          sum(when(path === "forest", 1L).otherwise(0L)), sum(when(path === "memory", 1L).otherwise(0L))).first()
        ((System.nanoTime() - started) / 1e9, r)
      }
      once()
      val runs = Seq(once(), once())
      val r = runs.head._2
      val (tp, fn, fp, tn) = (r.getLong(0), r.getLong(1), r.getLong(2), r.getLong(3))
      def f1(hits: Long) = if (2 * hits + fn + fp == 0) 0.0 else 2.0 * hits / (2 * hits + fn + fp)
      (runs.map(_._1).min, (f1(tp) + f1(tn)) / 2, 100.0 * r.getLong(4) / total, 100.0 * r.getLong(5) / total)
    }

    def show(name: String, result: (Double, Double, Double, Double)): Unit =
      println(f"RESULT $name%-58s ${result._1}%7.2f ${total / result._1}%,12.0f ${result._2}%8.4f ${result._3}%8.2f ${result._4}%8.2f")

    println("RESULT")
    println(f"RESULT $total%,d flows from the Kafka topic between ${from * 100}%.0f%% and ${to * 100}%.0f%% (six cores, best of two timed runs)")
    println(f"RESULT ${"method"}%-58s ${"seconds"}%7s ${"flows/s"}%12s ${"macro-F1"}%8s ${"forest %"}%8s ${"memory %"}%8s")
    show("random forest on every flow", timed(flows, forestAnswer(col("features")), lit("forest")))
    show("decision tree on every flow", timed(flows, treeAnswer(col("features")), lit("front")))

    val pattern = xxhash64(vector_to_array(col("features")))
    Seq(50, 10).foreach { clusters =>
      val gate =
        if (clusters == LiveGate.Clusters) LiveGate.load(spark)
        else { val (scaler, kmeans, limit) = LiveGate.fit(spark, clusters); new LiveGate(scaler, kmeans, limit) }
      val gated = gate.measure(flows)
      show(s"gate alone, $clusters clusters", timed(gated, col("unfamiliar").cast("double"), lit("gate")))
      show(s"no memory: tree for familiar, forest for unfamiliar, k=$clusters",
        timed(gated, when(col("unfamiliar"), forestAnswer(col("features"))).otherwise(treeAnswer(col("features"))),
          when(col("unfamiliar"), "forest").otherwise("front")))

      val candidates = gated.filter(col("unfamiliar")).withColumn("pattern", pattern).dropDuplicates("pattern")
        .withColumn("turn", row_number().over(Window.partitionBy("cluster").orderBy(rand(7))))
        .orderBy(col("turn"), rand(8)).limit(2000)
        .select("cluster", "gate_scaled", "is_attack").collect()
        .map(r => Labelled(r.getInt(0), r.getAs[Vector](1).toArray, r.getInt(2).toDouble))

      for (size <- Seq(200, 500, 1000, 2000); sameCluster <- Seq(false, true)) {
        val lookup = Memory.lookup(spark, candidates.take(size), sameCluster)
        val routed = gated
          .withColumn("nn", when(col("unfamiliar"), lookup(col("gate_scaled"), col("cluster"))))
          .withColumn("path", when(!col("unfamiliar"), "front").when(col("nn._1") <= gate.limit, "memory").otherwise("forest"))
          .withColumn("answer", when(col("path") === "front", treeAnswer(col("features")))
            .when(col("path") === "memory", col("nn._2")).otherwise(forestAnswer(col("features"))))
        show(f"router k=$clusters, $size%4d labels, ${if (sameCluster) "search own cluster" else "search all labels"}",
          timed(routed, col("answer"), col("path")))
      }
    }

    spark.stop()
  }
}
