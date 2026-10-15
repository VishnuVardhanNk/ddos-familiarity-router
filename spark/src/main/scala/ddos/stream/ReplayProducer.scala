package ddos.stream

import ddos.common.Session
import ddos.ingest.BuildFeatures
import org.apache.spark.sql.functions._

object ReplayProducer {
  def main(args: Array[String]): Unit = {
    val spark = Session("ReplayProducer")
    val fraction = args.headOption.map(_.toDouble).getOrElse(0.1)

    val columns = Seq("flow_id", "ts", "src_ip", "src_port", "dst_ip", "dst_port", "label_family", "is_attack") ++
      BuildFeatures.features

    spark.read.parquet("/data/silver")
      .filter(col("capture_day") === "2019-03-11")
      .sample(withReplacement = false, fraction, 7)
      .select(columns.map(col): _*)
      .repartition(1)
      .sortWithinPartitions("ts")
      .select(to_json(struct(columns.map(col): _*)).as("value"))
      .write.format("kafka")
      .option("kafka.bootstrap.servers", "kafka:9092")
      .option("topic", "flows")
      .save()

    spark.stop()
  }
}
