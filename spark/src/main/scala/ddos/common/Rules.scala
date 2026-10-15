package ddos.common

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

object Rules {
  private def c(name: String): Column = col(name)
  private val infinity = lit(Double.PositiveInfinity)

  private def perSecond(amount: Column, durationMicros: Column): Column =
    when(amount.isNotNull && durationMicros > 0, amount / durationMicros * 1e6)
      .when(amount > 0 && durationMicros === 0, infinity)
      .when(amount === 0 && durationMicros === 0, lit(Double.NaN))

  private def perPacket(total: Column, packets: Column): Column =
    when(total.isNotNull && packets > 0, total / packets).when(total.isNotNull && packets === 0, 0.0)

  private def perGap(total: Column, packets: Column): Column = when(packets > 1, total / (packets - 1))

  private def overGaps(mean: Column, packets: Column): Column = when(packets > 1, mean * (packets - 1))

  private def finite(x: Column): Column = !isnan(x) && x =!= infinity

  private val fwdPackets = c("tot_fwd_pkts")
  private val bwdPackets = c("tot_bwd_pkts")
  private val allPackets = fwdPackets + bwdPackets
  private val duration = c("flow_duration")

  val formulas: Seq[(String, Column)] = Seq(
    "pkt_len_var" -> c("pkt_len_std") * c("pkt_len_std"),
    "pkt_len_std" -> when(c("pkt_len_var") >= 0, sqrt(c("pkt_len_var"))),

    "fwd_seg_size_avg" -> c("fwd_pkt_len_mean"),
    "fwd_pkt_len_mean" -> c("fwd_seg_size_avg"),
    "bwd_seg_size_avg" -> c("bwd_pkt_len_mean"),
    "bwd_pkt_len_mean" -> c("bwd_seg_size_avg"),

    "subflow_fwd_pkts" -> fwdPackets,
    "tot_fwd_pkts" -> c("subflow_fwd_pkts"),
    "subflow_bwd_pkts" -> bwdPackets,
    "tot_bwd_pkts" -> c("subflow_bwd_pkts"),
    "subflow_fwd_byts" -> c("totlen_fwd_pkts"),
    "totlen_fwd_pkts" -> c("subflow_fwd_byts"),
    "subflow_bwd_byts" -> c("totlen_bwd_pkts"),
    "totlen_bwd_pkts" -> c("subflow_bwd_byts"),

    "totlen_fwd_pkts" -> c("fwd_pkt_len_mean") * fwdPackets,
    "fwd_pkt_len_mean" -> perPacket(c("totlen_fwd_pkts"), fwdPackets),
    "totlen_bwd_pkts" -> c("bwd_pkt_len_mean") * bwdPackets,
    "bwd_pkt_len_mean" -> perPacket(c("totlen_bwd_pkts"), bwdPackets),

    "flow_byts_s" -> perSecond(c("totlen_fwd_pkts") + c("totlen_bwd_pkts"), duration),
    "flow_pkts_s" -> perSecond(allPackets, duration),
    "flow_duration" -> when(c("flow_pkts_s") > 0 && c("flow_pkts_s") =!= infinity, allPackets / c("flow_pkts_s") * 1e6),
    "tot_fwd_pkts" -> when(duration > 0 && finite(c("fwd_pkts_s")), c("fwd_pkts_s") * duration / 1e6),
    "tot_bwd_pkts" -> when(duration > 0 && finite(c("bwd_pkts_s")), c("bwd_pkts_s") * duration / 1e6),

    "flow_duration" -> overGaps(c("flow_iat_mean"), allPackets),
    "flow_iat_mean" -> perGap(duration, allPackets),
    "fwd_iat_tot" -> overGaps(c("fwd_iat_mean"), fwdPackets),
    "fwd_iat_mean" -> perGap(c("fwd_iat_tot"), fwdPackets),
    "bwd_iat_tot" -> overGaps(c("bwd_iat_mean"), bwdPackets),
    "bwd_iat_mean" -> perGap(c("bwd_iat_tot"), bwdPackets),

    "pkt_len_max" -> when(bwdPackets === 0, c("fwd_pkt_len_max")).when(fwdPackets === 0, c("bwd_pkt_len_max"))
      .when(c("fwd_pkt_len_max").isNotNull && c("bwd_pkt_len_max").isNotNull, greatest(c("fwd_pkt_len_max"), c("bwd_pkt_len_max"))),
    "pkt_len_min" -> when(bwdPackets === 0, c("fwd_pkt_len_min")).when(fwdPackets === 0, c("bwd_pkt_len_min"))
      .when(fwdPackets > 0 && bwdPackets > 0 && c("fwd_pkt_len_min").isNotNull && c("bwd_pkt_len_min").isNotNull,
        least(c("fwd_pkt_len_min"), c("bwd_pkt_len_min"))),
    "down_up_ratio" -> when(fwdPackets > 0 && bwdPackets.isNotNull, floor(bwdPackets / fwdPackets)).when(fwdPackets === 0, 0.0)
  )

  def fill(df: DataFrame, passes: Int = 4): DataFrame = {
    val byTarget = formulas.groupBy(_._1).map { case (target, rules) => target -> rules.map(_._2) }
    (1 to passes).foldLeft(df) { (current, _) =>
      current.withColumns(byTarget.map { case (target, candidates) =>
        val value = coalesce(c(target) +: candidates: _*)
        target -> (if (Schema.isLong(target)) round(value) else value)
      })
    }
  }
}
