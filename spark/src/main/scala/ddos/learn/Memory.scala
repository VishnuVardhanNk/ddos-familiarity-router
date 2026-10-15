package ddos.learn

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class Labelled(cluster: Int, scaled: Array[Double], answer: Double)

object Memory {
  private val labelSchema = StructType(Seq(
    StructField("pattern", LongType), StructField("cluster", IntegerType),
    StructField("scaled", ArrayType(DoubleType)), StructField("answer", IntegerType)))

  def read(spark: SparkSession): Array[Labelled] =
    spark.read.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", "labels").load()
      .select(from_json(col("value").cast("string"), labelSchema).as("label")).select("label.*")
      .filter(col("scaled").isNotNull).dropDuplicates("pattern")
      .collect().map(r => Labelled(r.getInt(1), r.getSeq[Double](2).toArray, r.getInt(3).toDouble))

  def closest(points: Array[Labelled], x: Array[Double]): (Double, Double) = {
    var best = Double.MaxValue
    var label = 0.0
    var p = 0
    while (p < points.length) {
      val point = points(p).scaled
      var distance = 0.0
      var i = 0
      while (i < x.length && distance < best) {
        val gap = x(i) - point(i)
        distance += gap * gap
        i += 1
      }
      if (distance < best) {
        best = distance
        label = points(p).answer
      }
      p += 1
    }
    (math.sqrt(best), label)
  }

  def lookup(spark: SparkSession, points: Array[Labelled], sameCluster: Boolean) = {
    val whole = spark.sparkContext.broadcast(points)
    val grouped = spark.sparkContext.broadcast(points.groupBy(_.cluster))
    val nothing = Array.empty[Labelled]
    udf((v: Vector, cluster: Int) =>
      closest(if (sameCluster) grouped.value.getOrElse(cluster, nothing) else whole.value, v.toArray)).asNondeterministic()
  }

  def nearest(spark: SparkSession, points: Array[Labelled], rows: DataFrame, sameCluster: Boolean = true): DataFrame = {
    if (points.isEmpty) return rows.withColumn("nn_distance", lit(Double.MaxValue)).withColumn("nn_label", lit(0.0))
    rows.withColumn("nn", lookup(spark, points, sameCluster)(col("gate_scaled"), col("cluster")))
      .withColumn("nn_distance", col("nn._1")).withColumn("nn_label", col("nn._2")).drop("nn")
  }
}
