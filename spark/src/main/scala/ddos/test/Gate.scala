package ddos.test

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class FittedGate(measure: DataFrame => DataFrame, limits: Map[Double, Double],
                      scaled: DataFrame => DataFrame, centres: Seq[Vector])

object Gate {
  val models: Seq[String] =
    Seq("random_forest", "decision_tree", "neural_network", "gradient_boosted_trees", "logistic_regression")
  val ScoredPath = "/data/test/day2_scored"
  private val patternOf = xxhash64(vector_to_array(col("features")))

  def fit(spark: SparkSession, clusters: Int = 50, squash: Boolean = true, percentiles: Seq[Double] = Seq(0.99)): FittedGate = {
    val day1 = spark.read.parquet("/data/features").filter(col("capture_day") === "2019-01-12")

    val squashValues = udf((v: Vector) => Vectors.dense(v.toArray.map(x => math.signum(x) * math.log1p(math.abs(x)))))
    def prepared(rows: DataFrame): DataFrame =
      rows.withColumn("gate_input", if (squash) squashValues(col("features")) else col("features"))

    val day1Train = prepared(day1.filter(col("split") === "train"))
    val fitRows = day1Train.sample(withReplacement = false, math.min(1.0, 1000000.0 / day1Train.count()), 7).persist()
    val scaler = new StandardScaler().setInputCol("gate_input").setOutputCol("gate_scaled").setWithMean(true).fit(fitRows)
    val kmeans = new KMeans().setFeaturesCol("gate_scaled").setPredictionCol("cluster").setK(clusters).setSeed(7)
      .fit(scaler.transform(fitRows))
    fitRows.unpersist()

    val centres = kmeans.clusterCenters
    val distance = udf((v: Vector) => math.sqrt(centres.map(c => Vectors.sqdist(v, c)).min))
    def measure(rows: DataFrame): DataFrame =
      kmeans.transform(scaler.transform(prepared(rows))).withColumn("distance", distance(col("gate_scaled")))
        .drop("gate_input", "gate_scaled")

    val day1Distances = measure(day1.filter(col("split") === "test"))
    val limits = percentiles.zip(day1Distances.stat.approxQuantile("distance", percentiles.toArray, 0.0005)).toMap
    FittedGate(measure, limits, rows => scaler.transform(prepared(rows)).drop("gate_input"), centres.toSeq)
  }

  def scoredDay2(spark: SparkSession): DataFrame = {
    val files = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (!files.exists(new Path(ScoredPath + "/_SUCCESS"))) score(spark).write.mode("overwrite").parquet(ScoredPath)
    spark.read.parquet(ScoredPath)
  }

  private def score(spark: SparkSession): DataFrame = {
    val data = spark.read.parquet("/data/features")
    val day2 = data.filter(col("capture_day") === "2019-03-11")
    val gate = fit(spark)

    val day1Labels = data.filter(col("capture_day") === "2019-01-12").withColumn("pattern", patternOf)
      .groupBy("pattern").agg(min("is_attack").as("lowest"), max("is_attack").as("highest"))
      .select(col("pattern"), when(col("lowest") === col("highest"), col("lowest")).as("memory"))

    val detectors = models.map(name => name -> PipelineModel.load(s"/models/whole/cross_day/detection/$name"))
    detectors.foldLeft(gate.measure(day2).withColumn("unfamiliar", col("distance") > gate.limits(0.99))) { case (rows, (name, model)) =>
      model.transform(rows)
        .withColumn(name, col("prediction"))
        .withColumn(name + "_sure", array_max(vector_to_array(col("probability"))))
        .drop("rawPrediction", "probability", "prediction", "scaled_features")
    }.withColumn("pattern", patternOf)
      .drop("capture_day", "split", "family_index")
      .join(day1Labels, Seq("pattern"), "left")
  }
}
