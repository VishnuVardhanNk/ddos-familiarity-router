package ddos.test

import ddos.learn.{Gate => LiveGate}
import ddos.stream.LiveScorer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Sample {
  val Stored = "/data/test/stream_sample"
  val models: Seq[String] = LiveScorer.models
  val FlowsPerTick = 12000

  def load(spark: SparkSession): DataFrame = {
    val files = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    if (!files.exists(new Path(Stored + "/_SUCCESS"))) score(spark).write.mode("overwrite").parquet(Stored)
    spark.read.parquet(Stored)
  }

  private def score(spark: SparkSession): DataFrame = {
    val flows = spark.read.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", "flows").load()
      .select(col("offset"), from_json(col("value").cast("string"), LiveScorer.flowSchema).as("flow"))
      .select("offset", "flow.*")
      .repartition(12)

    val located = LiveGate.load(spark).measure(PipelineModel.load("/models/encoder").transform(flows))
    models.foldLeft(located) { (rows, name) =>
      PipelineModel.load(s"/models/whole/cross_day/detection/$name").transform(rows)
        .withColumn(name, col("prediction"))
        .drop("rawPrediction", "probability", "prediction", "scaled_features")
    }.drop("gate_scaled", "family_index", "flow_id", "src_ip", "dst_ip")
  }
}
