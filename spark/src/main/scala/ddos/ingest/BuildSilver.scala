package ddos.ingest

import ddos.common.{Rules, Schema, Session}
import org.apache.spark.sql.functions._

object BuildSilver {
  def main(args: Array[String]): Unit = {
    val spark = Session("BuildSilver", 96)
    val features = Schema.features.map(_.name)

    val bronze = spark.read.parquet("/data/bronze")

    val family = regexp_replace(trim(col("label")), "^DrDoS_", "")
    val labelled = bronze
      .withColumn("label_family",
        when(family === "UDP-lag", "UDPLag").when(upper(family) === "BENIGN", "Benign").otherwise(family))
      .withColumn("is_attack", (col("label_family") =!= "Benign").cast("int"))
      .withColumn("ts", col("timestamp").cast("timestamp"))

    val withFlags = labelled.withColumns(Schema.tcpFlagFeatures.map { f =>
      f -> when(col(f).isNull && col("protocol") =!= 6, 0L).otherwise(col(f))
    }.toMap)

    val key = "capture_day" +: Schema.connectionKey
    val keepFirstValue = withFlags.columns.filterNot(key.contains).map(c => first(col(c), ignoreNulls = true).as(c))
    val merged = withFlags.groupBy(key.map(col): _*).agg(keepFirstValue.head, keepFirstValue.tail: _*)

    val recovered = Rules.fill(merged.withColumns(features.map(f => f -> col(f).cast("double")).toMap))

    val day1Max = recovered.filter(col("capture_day") === "2019-01-12")
      .select(features.map(f => max(when(!isnan(col(f)) && col(f) =!= Double.PositiveInfinity, col(f))).as(f)): _*)
      .head()
    val finite = recovered.withColumns(features.map { f =>
      f -> when(isnan(col(f)), 0.0).when(col(f) === Double.PositiveInfinity, day1Max.getAs[Double](f)).otherwise(col(f))
    }.toMap)

    val medians = finite.groupBy("capture_day", "protocol")
      .agg(expr(s"percentile_approx(${features.head}, 0.5)").as(features.head + "_median"),
        features.tail.map(f => expr(s"percentile_approx($f, 0.5)").as(f + "_median")): _*)
    val filled = finite.join(medians, Seq("capture_day", "protocol"))
      .withColumns(features.map(f => f -> coalesce(col(f), col(f + "_median"))).toMap)
      .drop(features.map(_ + "_median"): _*)

    val silver = filled.withColumns(features.filter(Schema.isLong).map(f => f -> col(f).cast("long")).toMap)

    silver.write.mode("overwrite").partitionBy("capture_day").parquet("/data/silver")

    spark.stop()
  }
}
