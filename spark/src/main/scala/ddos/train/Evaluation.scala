package ddos.train

import ddos.common.{ClassScore, Confusion, Metrics}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

case class Summary(experiment: String, task: String, model: String, macro_f1: Double, benign_auc_pr: Double)

class Evaluation(val spark: SparkSession) {
  private val scores = ArrayBuffer[ClassScore]()
  private val confusions = ArrayBuffer[Confusion]()
  private val summaries = ArrayBuffer[Summary]()

  def evaluate(experiment: String, task: String, name: String, model: PipelineModel,
               test: DataFrame, labelCol: String, names: Seq[String]): Unit = {
    val predictions = model.transform(test).persist(StorageLevel.MEMORY_AND_DISK)
    val counts = Metrics.confusion(predictions, labelCol)
    val perClass = Metrics.classScores(counts, names, experiment, task, name)
    val auc =
      if (task == "detection") new BinaryClassificationEvaluator()
        .setLabelCol("benign_label").setRawPredictionCol("benign_score").setMetricName("areaUnderPR")
        .evaluate(predictions
          .withColumn("benign_label", lit(1.0) - col("is_attack"))
          .withColumn("benign_score", vector_to_array(col("probability"))(0)))
      else Double.NaN
    predictions.unpersist()

    scores ++= perClass
    confusions ++= counts.map { case ((a, p), n) => Confusion(experiment, task, name, names(a), names(p), n) }
    summaries += Summary(experiment, task, name, Metrics.macroF1(perClass), auc)

    println(f"\n$experiment / $task / $name: macro-F1 ${Metrics.macroF1(perClass)}%.4f, benign AUC-PR $auc%.4f")
    perClass.filter(_.support > 0).foreach { s =>
      println(f"    ${s.label}%-10s precision ${s.precision}%.4f  recall ${s.recall}%.4f  F1 ${s.f1}%.4f  (${s.support}%,d test rows)")
    }
  }

  def save(prefix: String): Unit = {
    import spark.implicits._
    scores.toDF().coalesce(1).write.mode("overwrite").parquet(s"/data/reports/${prefix}class_scores")
    confusions.toDF().coalesce(1).write.mode("overwrite").parquet(s"/data/reports/${prefix}confusion")
    summaries.toDF().coalesce(1).write.mode("overwrite").parquet(s"/data/reports/${prefix}model_summary")
  }
}
