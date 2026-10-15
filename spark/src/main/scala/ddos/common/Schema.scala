package ddos.common

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

case class Field(name: String, csv: String, json: String, dataType: DataType)

object Schema {
  private def str(n: String, c: String, j: String) = Field(n, c, j, StringType)
  private def lng(n: String, c: String, j: String) = Field(n, c, j, LongType)
  private def dbl(n: String, c: String, j: String) = Field(n, c, j, DoubleType)

  val identifiers: Seq[Field] = Seq(
    str("flow_id", "Flow ID", "flow_id"),
    str("timestamp", " Timestamp", "timestamp"),
    str("src_ip", " Source IP", "src_ip"),
    lng("src_port", " Source Port", "src_port"),
    str("dst_ip", " Destination IP", "dest_ip"),
    lng("dst_port", " Destination Port", "dest_port"),
    lng("protocol", " Protocol", "proto"),
    str("label", " Label", "label")
  )

  val features: Seq[Field] = Seq(
    lng("flow_duration", " Flow Duration", "flow.duration"),
    lng("tot_fwd_pkts", " Total Fwd Packets", "flow.tot_fwd_pkts"),
    lng("tot_bwd_pkts", " Total Backward Packets", "flow.tot_bwd_pkts"),
    dbl("totlen_fwd_pkts", "Total Length of Fwd Packets", "flow.totlen_fwd_bytes"),
    dbl("totlen_bwd_pkts", " Total Length of Bwd Packets", "flow.totlen_bwd_bytes"),

    dbl("fwd_pkt_len_max", " Fwd Packet Length Max", "packet_length.fwd_max"),
    dbl("fwd_pkt_len_min", " Fwd Packet Length Min", "packet_length.fwd_min"),
    dbl("fwd_pkt_len_mean", " Fwd Packet Length Mean", "packet_length.fwd_mean"),
    dbl("fwd_pkt_len_std", " Fwd Packet Length Std", "packet_length.fwd_std"),
    dbl("bwd_pkt_len_max", "Bwd Packet Length Max", "packet_length.bwd_max"),
    dbl("bwd_pkt_len_min", " Bwd Packet Length Min", "packet_length.bwd_min"),
    dbl("bwd_pkt_len_mean", " Bwd Packet Length Mean", "packet_length.bwd_mean"),
    dbl("bwd_pkt_len_std", " Bwd Packet Length Std", "packet_length.bwd_std"),
    dbl("pkt_len_min", " Min Packet Length", "packet_length.min_len"),
    dbl("pkt_len_max", " Max Packet Length", "packet_length.max_len"),
    dbl("pkt_len_mean", " Packet Length Mean", "packet_length.mean_len"),
    dbl("pkt_len_std", " Packet Length Std", "packet_length.std_len"),
    dbl("pkt_len_var", " Packet Length Variance", "packet_length.variance"),

    dbl("flow_byts_s", "Flow Bytes/s", "rate.flow_bytes_s"),
    dbl("flow_pkts_s", " Flow Packets/s", "rate.flow_packets_s"),
    dbl("fwd_pkts_s", "Fwd Packets/s", "rate.fwd_packets_s"),
    dbl("bwd_pkts_s", " Bwd Packets/s", "rate.bwd_packets_s"),

    dbl("flow_iat_mean", " Flow IAT Mean", "iat.flow_mean"),
    dbl("flow_iat_std", " Flow IAT Std", "iat.flow_std"),
    dbl("flow_iat_max", " Flow IAT Max", "iat.flow_max"),
    dbl("flow_iat_min", " Flow IAT Min", "iat.flow_min"),
    dbl("fwd_iat_tot", "Fwd IAT Total", "iat.fwd_total"),
    dbl("fwd_iat_mean", " Fwd IAT Mean", "iat.fwd_mean"),
    dbl("fwd_iat_std", " Fwd IAT Std", "iat.fwd_std"),
    dbl("fwd_iat_max", " Fwd IAT Max", "iat.fwd_max"),
    dbl("fwd_iat_min", " Fwd IAT Min", "iat.fwd_min"),
    dbl("bwd_iat_tot", "Bwd IAT Total", "iat.bwd_total"),
    dbl("bwd_iat_mean", " Bwd IAT Mean", "iat.bwd_mean"),
    dbl("bwd_iat_std", " Bwd IAT Std", "iat.bwd_std"),
    dbl("bwd_iat_max", " Bwd IAT Max", "iat.bwd_max"),
    dbl("bwd_iat_min", " Bwd IAT Min", "iat.bwd_min"),

    lng("fwd_psh_flags", "Fwd PSH Flags", "tcp_flags.fwd_psh"),
    lng("bwd_psh_flags", " Bwd PSH Flags", "tcp_flags.bwd_psh"),
    lng("fwd_urg_flags", " Fwd URG Flags", "tcp_flags.fwd_urg"),
    lng("bwd_urg_flags", " Bwd URG Flags", "tcp_flags.bwd_urg"),
    lng("fin_flag_cnt", "FIN Flag Count", "tcp_flags.fin_count"),
    lng("syn_flag_cnt", " SYN Flag Count", "tcp_flags.syn_count"),
    lng("rst_flag_cnt", " RST Flag Count", "tcp_flags.rst_count"),
    lng("psh_flag_cnt", " PSH Flag Count", "tcp_flags.psh_count"),
    lng("ack_flag_cnt", " ACK Flag Count", "tcp_flags.ack_count"),
    lng("urg_flag_cnt", " URG Flag Count", "tcp_flags.urg_count"),
    lng("cwe_flag_cnt", " CWE Flag Count", "tcp_flags.cwe_count"),
    lng("ece_flag_cnt", " ECE Flag Count", "tcp_flags.ece_count"),

    lng("fwd_header_len", " Fwd Header Length", "header.fwd_header_len"),
    lng("bwd_header_len", " Bwd Header Length", "header.bwd_header_len"),

    dbl("down_up_ratio", " Down/Up Ratio", "segment.down_up_ratio"),
    dbl("pkt_size_avg", " Average Packet Size", "segment.avg_packet_size"),
    dbl("fwd_seg_size_avg", " Avg Fwd Segment Size", "segment.avg_fwd_segment_size"),
    dbl("bwd_seg_size_avg", " Avg Bwd Segment Size", "segment.avg_bwd_segment_size"),

    dbl("fwd_byts_b_avg", "Fwd Avg Bytes/Bulk", "bulk.fwd_avg_bytes_bulk"),
    dbl("fwd_pkts_b_avg", " Fwd Avg Packets/Bulk", "bulk.fwd_avg_packets_bulk"),
    dbl("fwd_blk_rate_avg", " Fwd Avg Bulk Rate", "bulk.fwd_avg_bulk_rate"),
    dbl("bwd_byts_b_avg", " Bwd Avg Bytes/Bulk", "bulk.bwd_avg_bytes_bulk"),
    dbl("bwd_pkts_b_avg", " Bwd Avg Packets/Bulk", "bulk.bwd_avg_packets_bulk"),
    dbl("bwd_blk_rate_avg", "Bwd Avg Bulk Rate", "bulk.bwd_avg_bulk_rate"),

    lng("subflow_fwd_pkts", "Subflow Fwd Packets", "subflow.fwd_packets"),
    dbl("subflow_fwd_byts", " Subflow Fwd Bytes", "subflow.fwd_bytes"),
    lng("subflow_bwd_pkts", " Subflow Bwd Packets", "subflow.bwd_packets"),
    dbl("subflow_bwd_byts", " Subflow Bwd Bytes", "subflow.bwd_bytes"),

    lng("init_fwd_win_byts", "Init_Win_bytes_forward", "window.init_win_fwd"),
    lng("init_bwd_win_byts", " Init_Win_bytes_backward", "window.init_win_bwd"),
    lng("fwd_act_data_pkts", " act_data_pkt_fwd", "window.act_data_pkt_fwd"),
    lng("fwd_seg_size_min", " min_seg_size_forward", "window.min_seg_size_fwd"),

    dbl("active_mean", "Active Mean", "activity.active_mean"),
    dbl("active_std", " Active Std", "activity.active_std"),
    dbl("active_max", " Active Max", "activity.active_max"),
    dbl("active_min", " Active Min", "activity.active_min"),
    dbl("idle_mean", "Idle Mean", "activity.idle_mean"),
    dbl("idle_std", " Idle Std", "activity.idle_std"),
    dbl("idle_max", " Idle Max", "activity.idle_max"),
    dbl("idle_min", " Idle Min", "activity.idle_min"),

    lng("inbound", " Inbound", "misc.inbound")
  )

  val extras: Seq[Field] = Seq(
    str("similar_http", "SimillarHTTP", "misc.similar_http")
  )

  val all: Seq[Field] = identifiers ++ features ++ extras

  val tcpFlagFeatures: Seq[String] = features.map(_.name).filter(n => n.endsWith("_flags") || n.endsWith("_flag_cnt"))

  val connectionKey: Seq[String] = Seq("flow_id", "timestamp", "src_ip", "src_port", "dst_ip", "dst_port", "protocol")

  def isLong(name: String): Boolean = features.exists(f => f.name == name && f.dataType == LongType)

  def jsonSchema: StructType = {
    def jsonType(f: Field): DataType = if (f.dataType == StringType) StringType else DoubleType
    val top = all.filterNot(_.json.contains(".")).map(f => StructField(f.json, jsonType(f)))
    val nested = all.filter(_.json.contains("."))
    val groups = nested.map(_.json.split('.')(0)).distinct.map { g =>
      val inner = nested.filter(_.json.startsWith(g + ".")).map(f => StructField(f.json.split('.')(1), jsonType(f)))
      StructField(g, StructType(inner))
    }
    StructType(top ++ groups)
  }

  def fromCsv(df: DataFrame): DataFrame = standardise(df, f => col(s"`${f.csv}`"))

  def fromJson(df: DataFrame): DataFrame = standardise(df, f => col(f.json))

  private def typed(c: Column, f: Field): Column = f.dataType match {
    case LongType => c.cast(DoubleType).cast(LongType)
    case t => c.cast(t)
  }

  private def standardise(df: DataFrame, source: Field => Column): DataFrame = {
    val fields = all.map(f => typed(source(f), f).as(f.name))
    df.select(fields :+ element_at(split(input_file_name(), "/"), -1).as("source_file"): _*)
  }
}
