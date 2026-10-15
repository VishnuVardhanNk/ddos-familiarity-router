package ddos.ingest

import ddos.common.Session
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object BuildFeatures {
  val features: Seq[String] = Seq(
    "protocol",
    "flow_duration", "tot_fwd_pkts", "tot_bwd_pkts", "totlen_fwd_pkts", "totlen_bwd_pkts",
    "fwd_pkt_len_max", "fwd_pkt_len_min", "fwd_pkt_len_mean", "fwd_pkt_len_std",
    "bwd_pkt_len_max", "bwd_pkt_len_min", "bwd_pkt_len_mean", "bwd_pkt_len_std",
    "pkt_len_min", "pkt_len_max", "pkt_len_mean", "pkt_len_std", "pkt_len_var",
    "flow_byts_s", "flow_pkts_s", "fwd_pkts_s", "bwd_pkts_s",
    "flow_iat_mean", "flow_iat_std", "flow_iat_max", "flow_iat_min",
    "fwd_iat_tot", "fwd_iat_mean", "fwd_iat_std", "fwd_iat_max", "fwd_iat_min",
    "bwd_iat_tot", "bwd_iat_mean", "bwd_iat_std", "bwd_iat_max", "bwd_iat_min",
    "fwd_psh_flags", "syn_flag_cnt", "ack_flag_cnt", "urg_flag_cnt", "cwe_flag_cnt",
    "fwd_header_len", "bwd_header_len",
    "down_up_ratio", "pkt_size_avg", "fwd_act_data_pkts",
    "active_mean", "active_std", "active_max", "active_min",
    "idle_mean", "idle_std", "idle_max", "idle_min"
  )

  def main(args: Array[String]): Unit = {
    val spark = Session("BuildFeatures", 96)

    val vectors = spark.read.parquet("/data/silver")
      .groupBy((Seq("capture_day", "label_family", "is_attack") ++ features).map(col): _*)
      .agg(count(lit(1)).as("occurrences"))
      .withColumn("split",
        when(pmod(xxhash64(("capture_day" +: features).map(col): _*), lit(100)) < 80, "train").otherwise("test"))
      .persist(StorageLevel.DISK_ONLY)

    val encoder = new Pipeline().setStages(Array(
      new StringIndexer().setInputCol("label_family").setOutputCol("family_index").setHandleInvalid("keep"),
      new VectorAssembler().setInputCols(features.toArray).setOutputCol("features")
    )).fit(vectors.filter(col("split") === "train"))
    encoder.write.overwrite().save("/models/encoder")

    encoder.transform(vectors)
      .select(col("label_family"), col("family_index"), col("is_attack").cast("double").as("is_attack"),
        col("occurrences"), col("features"), col("capture_day"), col("split"))
      .write.mode("overwrite").partitionBy("capture_day", "split").parquet("/data/features")

    spark.stop()
  }
}
