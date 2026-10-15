package ddos.common

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

case class ClassScore(experiment: String, task: String, model: String, label: String,
                      precision: Double, recall: Double, f1: Double, support: Long)

case class Confusion(experiment: String, task: String, model: String, actual: String, predicted: String, count: Long)

object Metrics {
  def confusion(predictions: DataFrame, labelCol: String): Map[(Int, Int), Long] =
    predictions.groupBy(col(labelCol).cast("int"), col("prediction").cast("int")).count()
      .collect().map(r => (r.getInt(0), r.getInt(1)) -> r.getLong(2)).toMap

  def classScores(counts: Map[(Int, Int), Long], names: Seq[String],
                  experiment: String, task: String, model: String): Seq[ClassScore] =
    names.indices.map { k =>
      val correct = counts.getOrElse((k, k), 0L).toDouble
      val predicted = counts.collect { case ((_, p), n) if p == k => n }.sum.toDouble
      val actual = counts.collect { case ((a, _), n) if a == k => n }.sum
      val precision = if (predicted > 0) correct / predicted else 0.0
      val recall = if (actual > 0) correct / actual else 0.0
      val f1 = if (precision + recall > 0) 2 * precision * recall / (precision + recall) else 0.0
      ClassScore(experiment, task, model, names(k), precision, recall, f1, actual)
    }

  def macroF1(scores: Seq[ClassScore]): Double = {
    val present = scores.filter(_.support > 0)
    present.map(_.f1).sum / present.size
  }
}
