package ddos.stream

import ddos.common.{Postgres, Session}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object LoadReports {
  def main(args: Array[String]): Unit = {
    val spark = Session("LoadReports")

    def report(name: String): DataFrame = spark.read.parquet(s"/data/reports/$name")

    def bothTrainings(name: String): DataFrame =
      report(name).withColumn("training", lit("sample"))
        .unionByName(report("whole_" + name).withColumn("training", lit("all_rows")))

    Postgres.write(
      bothTrainings("model_summary")
        .withColumn("benign_auc_pr", nanvl(col("benign_auc_pr"), lit(null).cast("double"))),
      "model_summary")
    Postgres.write(bothTrainings("class_scores"), "class_scores")
    Postgres.write(bothTrainings("confusion"), "confusion")

    Seq("tuning", "feature_importance", "feature_list").foreach(name => Postgres.write(report(name), name))

    spark.stop()
  }
}
