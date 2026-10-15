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

import scala.collection.mutable

object EfficiencyCheck {
  val Chunk = 120000L
  val LabelsPerChunk = 240

  case class Work(var seconds: Double = 0, all: Array[Long] = new Array[Long](4), far: Array[Long] = new Array[Long](4),
                  var forest: Long = 0, var memory: Long = 0)

  def macroF1(c: Array[Long]): Double = {
    def f1(hits: Long) = if (2 * hits + c(1) + c(2) == 0) 0.0 else 2.0 * hits / (2 * hits + c(1) + c(2))
    (f1(c(0)) + f1(c(3))) / 2
  }

  def cells(answer: Column, where: Column): Seq[Column] =
    Seq((1, 1.0), (1, 0.0), (0, 1.0), (0, 0.0)).map { case (attack, said) =>
      coalesce(sum(when(where && col("is_attack") === attack && answer === said, 1L).otherwise(0L)), lit(0L))
    }

  def main(args: Array[String]): Unit = {
    val spark = Session("EfficiencyCheck", 6)

    val encoder = PipelineModel.load("/models/encoder")
    val gate = LiveGate.load(spark)
    val forest = PipelineModel.load("/models/whole/cross_day/detection/random_forest").stages(0)
      .asInstanceOf[RandomForestClassificationModel]
    val tree = PipelineModel.load("/models/whole/cross_day/detection/decision_tree").stages(0)
      .asInstanceOf[DecisionTreeClassificationModel]
    val forestAnswer = udf((v: Vector) => forest.predict(v)).asNondeterministic()
    val treeAnswer = udf((v: Vector) => tree.predict(v)).asNondeterministic()
    val pattern = xxhash64(vector_to_array(col("features")))
    val total = "\\d+".r.findAllIn(LiveScorer.startingOffsets(1.0)).toSeq.last.toLong
    def at(offset: Long) = s"""{"flows":{"0":$offset}}"""

    val forestOnly = Work()
    val routerOnly = Work()
    val withCopying = Work()
    val memory = mutable.ArrayBuffer[Labelled]()
    val asked = mutable.Set[Long]()
    var asking = 0.0
    var askingChunks = 0

    def measure(work: Work, rows: DataFrame): Long = {
      val started = System.nanoTime()
      val sums = cells(col("answer"), lit(true)) ++ cells(col("answer"), col("path") =!= "front") ++
        Seq("forest", "memory").map(p => coalesce(sum(when(col("path") === p, 1L).otherwise(0L)), lit(0L)))
      val row = rows.agg(sums.head, sums.tail: _*).first()
      work.seconds += (System.nanoTime() - started) / 1e9
      (0 until 4).foreach { i => work.all(i) += row.getLong(i); work.far(i) += row.getLong(4 + i) }
      work.forest += row.getLong(8)
      work.memory += row.getLong(9)
      row.getLong(8)
    }

    var warm = false
    (0L until total by Chunk).foreach { from =>
      val flows = encoder.transform(
        spark.read.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", "flows")
          .option("startingOffsets", at(from)).option("endingOffsets", at(math.min(total, from + Chunk))).load()
          .select(from_json(col("value").cast("string"), LiveScorer.flowSchema).as("flow")).select("flow.*"))
        .select("features", "is_attack").repartition(6).persist(StorageLevel.MEMORY_ONLY)
      val size = flows.count()

      val everything = flows.withColumn("path", lit("forest")).withColumn("answer", forestAnswer(col("features")))
      val gated = gate.measure(flows)
      val plain = gated.withColumn("path", when(col("unfamiliar"), "forest").otherwise("front"))
        .withColumn("answer", when(col("unfamiliar"), forestAnswer(col("features"))).otherwise(treeAnswer(col("features"))))
      if (!warm) { measure(Work(), everything); measure(Work(), plain); warm = true }

      measure(forestOnly, everything)
      measure(routerOnly, plain)

      val lookup = Memory.lookup(spark, memory.toArray, sameCluster = true)
      val placed = gated.withColumn("nn", when(col("unfamiliar"), lookup(col("gate_scaled"), col("cluster"))))
        .withColumn("path", when(!col("unfamiliar"), "front").when(col("nn._1") <= gate.limit, "memory").otherwise("forest"))
      val copied = placed.withColumn("answer", when(col("path") === "front", treeAnswer(col("features")))
        .when(col("path") === "memory", col("nn._2")).otherwise(forestAnswer(col("features"))))
      val forestFlows = measure(withCopying, copied)

      if (forestFlows > LiveScorer.ForestBusy * size && asked.size < LiveScorer.MemoryLabels) {
        val started = System.nanoTime()
        val fresh = placed.filter(col("path") === "forest").withColumn("pattern", pattern)
          .filter(!col("pattern").isin(asked.toSeq: _*)).dropDuplicates("pattern")
          .withColumn("turn", row_number().over(Window.partitionBy("cluster").orderBy(rand(7))))
          .orderBy(col("turn"), rand(8)).limit(math.min(LabelsPerChunk, LiveScorer.MemoryLabels - asked.size))
          .select("pattern", "cluster", "gate_scaled", "is_attack").collect()
        memory ++= fresh.map(r => Labelled(r.getInt(1), r.getAs[Vector](2).toArray, r.getInt(3).toDouble))
        asked ++= fresh.map(_.getLong(0))
        asking += (System.nanoTime() - started) / 1e9
        askingChunks += 1
      }
      flows.unpersist()
    }

    val models = Sample.models
    val sample = Sample.load(spark)
    val modelSums = models.flatMap(m => cells(col(m), lit(true)) ++ cells(col(m), !col("unfamiliar")))
    val modelRow = sample.agg(modelSums.head, modelSums.tail: _*).first()
    val alone = models.indices.map(i => (0 until 4).map(k => modelRow.getLong(8 * i + k)).toArray)
    val familiar = models.indices.map(i => (0 until 4).map(k => modelRow.getLong(8 * i + 4 + k)).toArray)
    def joined(near: Array[Long], far: Array[Long]) = near.zip(far).map { case (a, b) => a + b }

    def pct(part: Long) = 100.0 * part / total
    println("RESULT")
    println(f"RESULT $total%,d connections in stream order, in chunks of $Chunk%,d (six cores, model work only)")
    println("RESULT Labels are asked only for connections sent to the forest and are used from the next chunk on.")
    println("RESULT")
    println(f"RESULT ${"method"}%-46s ${"seconds"}%8s ${"flows/s"}%10s ${"forest %"}%9s ${"copied %"}%9s ${"front %"}%8s ${"speed"}%6s ${"macro-F1"}%9s")
    Seq("Random forest on every connection" -> forestOnly,
      "Router: tree if familiar, forest if not" -> routerOnly,
      "Router with label copying" -> withCopying).foreach { case (name, w) =>
      println(f"RESULT $name%-46s ${w.seconds}%8.1f ${total / w.seconds}%,10.0f ${pct(w.forest)}%9.2f ${pct(w.memory)}%9.2f " +
        f"${100 - pct(w.forest) - pct(w.memory)}%8.2f ${forestOnly.seconds / w.seconds}%6.2f ${macroF1(w.all)}%9.4f")
    }
    println(f"RESULT Labels asked: ${asked.size}%,d (${100.0 * asked.size / total}%.4f%% of connections)")
    println(f"RESULT Choosing which labels to ask, in $askingChunks of ${(total + Chunk - 1) / Chunk} chunks: $asking%.1f seconds in total")
    println(f"RESULT Unfamiliar connections: ${pct(routerOnly.forest)}%.2f%%")

    println("RESULT")
    println("RESULT Each model in front: alone, with the router, and with the router plus label copying")
    println(f"RESULT ${"model in front"}%-24s ${"alone"}%8s ${"router"}%8s ${"+ copying"}%10s ${"missed alone"}%13s ${"missed router"}%14s ${"missed + copying"}%17s " +
      f"${"false alarms alone"}%19s ${"router"}%8s ${"+ copying"}%10s")
    models.indices.foreach { i =>
      val routed = joined(familiar(i), routerOnly.far)
      val copying = joined(familiar(i), withCopying.far)
      println(f"RESULT ${models(i)}%-24s ${macroF1(alone(i))}%8.4f ${macroF1(routed)}%8.4f ${macroF1(copying)}%10.4f " +
        f"${alone(i)(1)}%,13d ${routed(1)}%,14d ${copying(1)}%,17d ${alone(i)(2)}%,19d ${routed(2)}%,8d ${copying(2)}%,10d")
    }

    spark.stop()
  }
}
