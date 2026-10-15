package ddos.learn

import ddos.common.Session
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class Gate(scaler: StandardScalerModel, kmeans: KMeansModel, val limit: Double) {
  def measure(rows: DataFrame): DataFrame = {
    val mean = scaler.mean.toArray
    val spread = scaler.std.toArray
    val centres = kmeans.clusterCenters.map(_.toArray)

    val locate = udf((v: Vector) => {
      val x = v.toArray
      val scaled = new Array[Double](x.length)
      var i = 0
      while (i < x.length) {
        val squashed = math.signum(x(i)) * math.log1p(math.abs(x(i)))
        scaled(i) = if (spread(i) == 0.0) 0.0 else (squashed - mean(i)) / spread(i)
        i += 1
      }
      var nearest = 0
      var best = Double.MaxValue
      var c = 0
      while (c < centres.length) {
        val centre = centres(c)
        var distance = 0.0
        var j = 0
        while (j < scaled.length && distance < best) {
          val gap = scaled(j) - centre(j)
          distance += gap * gap
          j += 1
        }
        if (distance < best) {
          best = distance
          nearest = c
        }
        c += 1
      }
      (nearest, math.sqrt(best), Vectors.dense(scaled))
    }).asNondeterministic()

    rows.withColumn("place", locate(col("features")))
      .withColumn("cluster", col("place._1"))
      .withColumn("distance", col("place._2"))
      .withColumn("gate_scaled", col("place._3"))
      .withColumn("unfamiliar", col("distance") > limit)
      .drop("place")
  }
}

object Gate {
  val Path = "/models/gate"
  val Clusters = 50
  val squash = udf((v: Vector) => Vectors.dense(v.toArray.map(x => math.signum(x) * math.log1p(math.abs(x)))))

  def load(spark: SparkSession): Gate = new Gate(
    StandardScalerModel.load(Path + "/scaler"), KMeansModel.load(Path + "/kmeans"),
    spark.read.parquet(Path + "/limit").first().getDouble(0))

  def fit(spark: SparkSession, clusters: Int): (StandardScalerModel, KMeansModel, Double) = {
    val day1 = spark.read.parquet("/data/features").filter(col("capture_day") === "2019-01-12")
    val train = day1.filter(col("split") === "train").withColumn("gate_input", squash(col("features")))
    val sample = train.sample(withReplacement = false, math.min(1.0, 1000000.0 / train.count()), 7).persist()

    val scaler = new StandardScaler().setInputCol("gate_input").setOutputCol("gate_scaled").setWithMean(true).fit(sample)
    val kmeans = new KMeans().setFeaturesCol("gate_scaled").setPredictionCol("cluster").setK(clusters).setSeed(7)
      .fit(scaler.transform(sample))
    sample.unpersist()
    val limit = new Gate(scaler, kmeans, Double.MaxValue).measure(day1.filter(col("split") === "test"))
      .stat.approxQuantile("distance", Array(0.99), 0.0005).head
    (scaler, kmeans, limit)
  }

  def main(args: Array[String]): Unit = {
    val spark = Session("BuildGate", 24)
    import spark.implicits._

    val (scaler, kmeans, limit) = fit(spark, Clusters)
    scaler.write.overwrite().save(Path + "/scaler")
    kmeans.write.overwrite().save(Path + "/kmeans")
    Seq(limit).toDF("limit").write.mode("overwrite").parquet(Path + "/limit")
    println(f"Gate saved: $Clusters clusters, unfamiliar beyond $limit%.3f")

    spark.stop()
  }
}
