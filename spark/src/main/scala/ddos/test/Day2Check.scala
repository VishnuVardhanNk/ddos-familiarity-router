package ddos.test

import ddos.common.Session
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

case class Cell(model: String, cluster: Int, unfamiliar: Boolean, unsure: Boolean, tp: Long, fn: Long, fp: Long, tn: Long) {
  def flows: Long = tp + fn + fp + tn
  def wrong: Long = fn + fp
}

object Day2Check {
  val models: Seq[String] = Gate.models
  val SureAbove = 0.9

  def macroF1(cells: Seq[Cell]): Double = {
    val tp = cells.map(_.tp).sum.toDouble
    val tn = cells.map(_.tn).sum.toDouble
    val errors = cells.map(_.wrong).sum.toDouble
    def f1(hits: Double) = if (2 * hits + errors == 0) 0.0 else 2 * hits / (2 * hits + errors)
    (f1(tp) + f1(tn)) / 2
  }

  def wrongPct(cells: Seq[Cell]): Double = 100.0 * cells.map(_.wrong).sum / math.max(1L, cells.map(_.flows).sum)

  def share(part: Long, whole: Long): Double = 100.0 * part / math.max(1L, whole)

  def longAt(row: Row, i: Int): Long = if (row.isNullAt(i)) 0L else row.getLong(i)

  def main(args: Array[String]): Unit = {
    val spark = Session("Day2Check", 48)
    import spark.implicits._

    val scored = Gate.scoredDay2(spark).drop("features").persist(StorageLevel.DISK_ONLY)
    val day1 = spark.read.parquet("/data/features").filter(col("capture_day") === "2019-01-12")

    val perModel = scored.select(col("cluster"), col("unfamiliar"), col("is_attack"), col("occurrences"),
      explode(array(models.map(m =>
        struct(lit(m).as("model"), col(m).as("predicted"), (col(m + "_sure") < SureAbove).as("unsure"))): _*)).as("p"))
    def flowsWhere(attack: Double, predicted: Double) =
      sum(when(col("is_attack") === attack && col("p.predicted") === predicted, col("occurrences")).otherwise(0L))
    val cells = perModel
      .groupBy(col("p.model").as("model"), col("cluster"), col("unfamiliar"), col("p.unsure").as("unsure"))
      .agg(flowsWhere(1.0, 1.0).as("tp"), flowsWhere(1.0, 0.0).as("fn"), flowsWhere(0.0, 1.0).as("fp"), flowsWhere(0.0, 0.0).as("tn"))
      .as[Cell].collect().toSeq

    val families = scored.groupBy("label_family").agg(
      sum("occurrences").as("flows"),
      (sum(when(col("unfamiliar"), col("occurrences")).otherwise(0L)) * 100.0 / sum("occurrences")).as("unfamiliar_pct")
    ).orderBy(desc("flows")).collect()

    val day2Patterns = scored.groupBy("pattern").agg(
      sum("occurrences").as("flows"), min("is_attack").as("lowest"), max("is_attack").as("highest")).persist()
    val patternCount = day2Patterns.count()
    val top = day2Patterns.orderBy(desc("flows")).limit(10000).select("flows").as[Long].collect()
    val mixedFlows = longAt(day2Patterns.filter(col("lowest") =!= col("highest")).agg(sum("flows")).first(), 0)

    val day1Patterns = day1.withColumn("pattern", xxhash64(vector_to_array(col("features"))))
      .groupBy("pattern").agg(min("is_attack").as("day1_lowest"), max("is_attack").as("day1_highest"))
    val seen = scored.join(day1Patterns, "pattern").agg(
      sum("occurrences"),
      sum(when(col("day1_lowest") === col("day1_highest") && col("day1_lowest") === col("is_attack"), col("occurrences"))
        .otherwise(0L))).first()

    val totalFlows = cells.filter(_.model == models.head).map(_.flows).sum
    val unfamiliarFlows = cells.filter(c => c.model == models.head && c.unfamiliar).map(_.flows).sum

    println("RESULT")
    println(f"RESULT Day 2: $totalFlows%,d flows in $patternCount%,d distinct patterns")
    println("RESULT Gate: a flow is unfamiliar when it is further from every day-1 cluster than 99% of day-1 traffic")
    println(f"RESULT Unfamiliar flows: ${share(unfamiliarFlows, totalFlows)}%.1f%% of day 2")
    println("RESULT")
    println("RESULT 1a. Unfamiliar traffic by true attack type")
    println(f"RESULT ${"type"}%-12s ${"flows"}%14s ${"unfamiliar %"}%14s")
    families.foreach(r => println(f"RESULT ${r.getString(0)}%-12s ${r.getLong(1)}%,14d ${r.getDouble(2)}%14.1f"))

    println("RESULT")
    println("RESULT 1b. Gate: are the models worse on unfamiliar flows?")
    println(f"RESULT ${"model"}%-24s ${"macro-F1"}%9s ${"wrong % familiar"}%18s ${"wrong % unfamiliar"}%20s ${"mistakes in unfamiliar %"}%25s")
    models.foreach { m =>
      val mine = cells.filter(_.model == m)
      val far = mine.filter(_.unfamiliar)
      println(f"RESULT $m%-24s ${macroF1(mine)}%9.4f ${wrongPct(mine.filter(!_.unfamiliar))}%18.2f ${wrongPct(far)}%20.2f " +
        f"${share(far.map(_.wrong).sum, mine.map(_.wrong).sum)}%25.1f")
    }

    println("RESULT")
    println("RESULT 2. Pattern memory: how repetitive is day 2?")
    Seq(100, 1000, 10000).foreach(n =>
      println(f"RESULT The $n%,6d most common patterns cover ${share(top.take(n).sum, totalFlows)}%.1f%% of flows"))
    println(f"RESULT Flows whose pattern carries both benign and attack labels: ${share(mixedFlows, totalFlows)}%.3f%%")
    println(f"RESULT Flows whose exact pattern was already seen on day 1: ${share(longAt(seen, 0), totalFlows)}%.1f%%")
    println(f"RESULT   of those, the day-1 label is right for: ${share(longAt(seen, 1), longAt(seen, 0))}%.2f%%")

    println("RESULT")
    println("RESULT 3. Trust map: best model per cluster, chosen with hindsight (an upper limit)")
    val chosen = cells.groupBy(_.cluster).values.toSeq.map { inCluster =>
      val best = models.minBy(m => inCluster.filter(_.model == m).map(_.wrong).sum)
      best -> inCluster.filter(_.model == best)
    }
    println(f"RESULT Best single model:      ${models.map(m => macroF1(cells.filter(_.model == m))).max}%.4f macro-F1")
    println(f"RESULT Best model per cluster: ${macroF1(chosen.flatMap(_._2))}%.4f macro-F1")
    models.foreach(m => println(f"RESULT   $m%-24s trusted in ${chosen.count(_._1 == m)}%2d of ${chosen.size} clusters"))

    println("RESULT")
    println(f"RESULT 4. Confidence: are the models worse when less than ${SureAbove * 100}%.0f%% sure?")
    println(f"RESULT ${"model"}%-24s ${"unsure flows %"}%15s ${"wrong % sure"}%13s ${"wrong % unsure"}%15s ${"mistakes in unsure %"}%21s")
    models.foreach { m =>
      val mine = cells.filter(_.model == m)
      val unsure = mine.filter(_.unsure)
      println(f"RESULT $m%-24s ${share(unsure.map(_.flows).sum, mine.map(_.flows).sum)}%15.2f ${wrongPct(mine.filter(!_.unsure))}%13.2f " +
        f"${wrongPct(unsure)}%15.2f ${share(unsure.map(_.wrong).sum, mine.map(_.wrong).sum)}%21.1f")
    }

    spark.stop()
  }
}
