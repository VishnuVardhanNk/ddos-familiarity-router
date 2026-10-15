package ddos.ingest

import ddos.common.{Schema, Session}
import org.apache.spark.sql.functions._

object BuildBronze {
  def main(args: Array[String]): Unit = {
    val spark = Session("BuildBronze")

    val day1 = Schema.fromCsv(spark.read.option("header", "true").csv("/data/raw/01-12"))
      .withColumn("capture_day", lit("2019-01-12"))
      .withColumn("source_format", lit("csv"))

    val day2 = Schema.fromJson(spark.read.schema(Schema.jsonSchema).json("/data/raw/03-11-json"))
      .withColumn("capture_day", lit("2019-03-11"))
      .withColumn("source_format", lit("json"))

    day1.unionByName(day2)
      .write.mode("overwrite").partitionBy("capture_day").parquet("/data/bronze")

    spark.stop()
  }
}
