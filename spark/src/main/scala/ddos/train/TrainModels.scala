package ddos.train

import ddos.common.Session
import ddos.ingest.BuildFeatures
import org.apache.spark.ml.{Estimator, Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StandardScaler, StringIndexerModel}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit, TrainValidationSplitModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

case class TuningResult(experiment: String, model: String, settings: String, validation_benign_f1: Double, chosen: Boolean)

object TrainModels {
  val AttacksPerBenign = 20

  private val benignF1 = new MulticlassClassificationEvaluator()
    .setLabelCol("is_attack").setMetricName("fMeasureByLabel").setMetricLabel(0.0)

  private def tuned(estimator: Estimator[_], grid: Array[ParamMap]): TrainValidationSplit =
    new TrainValidationSplit().setEstimator(estimator).setEstimatorParamMaps(grid)
      .setEvaluator(benignF1).setTrainRatio(0.75).setSeed(7)

  def detectors: Seq[(String, PipelineStage)] = {
    val forest = new RandomForestClassifier()
      .setLabelCol("is_attack").setWeightCol("weight").setSubsamplingRate(0.8).setSeed(7)
    val boosted = new GBTClassifier()
      .setLabelCol("is_attack").setWeightCol("weight").setSeed(7)

    Seq(
      "logistic_regression" -> new LogisticRegression()
        .setLabelCol("is_attack").setWeightCol("weight").setMaxIter(100),
      "decision_tree" -> new DecisionTreeClassifier()
        .setLabelCol("is_attack").setWeightCol("weight").setMaxDepth(12),
      "random_forest" -> tuned(forest, new ParamGridBuilder()
        .addGrid(forest.maxDepth, Array(8, 12, 16)).addGrid(forest.numTrees, Array(50, 100)).build()),
      "gradient_boosted_trees" -> tuned(boosted, new ParamGridBuilder()
        .addGrid(boosted.maxDepth, Array(4, 6)).addGrid(boosted.maxIter, Array(20, 40)).build()),
      "neural_network" -> new Pipeline().setStages(Array(
        new StandardScaler().setInputCol("features").setOutputCol("scaled_features"),
        new MultilayerPerceptronClassifier().setFeaturesCol("scaled_features").setLabelCol("is_attack")
          .setLayers(Array(BuildFeatures.features.size, 64, 32, 2)).setMaxIter(100).setSeed(7)))
    )
  }

  def balanced(rows: DataFrame): DataFrame = {
    val benign = rows.filter(col("is_attack") === 0.0)
    val attacks = rows.filter(col("is_attack") === 1.0)
    val nBenign = benign.count()
    val fraction = math.min(1.0, nBenign * AttacksPerBenign.toDouble / attacks.count())
    val sample = benign.unionByName(attacks.sample(withReplacement = false, fraction, 7))
    val nAttack = sample.count() - nBenign
    val total = (nBenign + nAttack).toDouble
    println(f"  training on $nBenign%,d benign + $nAttack%,d attack rows")
    sample.withColumn("weight", when(col("is_attack") === 0.0, total / (2 * nBenign)).otherwise(total / (2 * nAttack)))
      .persist(StorageLevel.MEMORY_AND_DISK)
  }

  def fit(stage: PipelineStage, train: DataFrame): PipelineModel = new Pipeline().setStages(Array(stage)).fit(train)

  def main(args: Array[String]): Unit = {
    val spark = Session("TrainModels", 96)
    import spark.implicits._

    val data = spark.read.parquet("/data/features")
    val families = PipelineModel.load("/models/encoder").stages(0).asInstanceOf[StringIndexerModel].labelsArray(0).toSeq
    val detectionClasses = Seq("benign", "attack")
    val results = new Evaluation(spark)
    val tuning = ArrayBuffer[TuningResult]()

    def recordTuning(experiment: String, name: String, model: PipelineModel): Unit = model.stages(0) match {
      case search: TrainValidationSplitModel =>
        val best = search.validationMetrics.max
        search.getEstimatorParamMaps.zip(search.validationMetrics).foreach { case (params, metric) =>
          val settings = params.toSeq.map(p => s"${p.param.name}=${p.value}").sorted.mkString(", ")
          tuning += TuningResult(experiment, name, settings, metric, metric == best)
          println(f"    tuning $name%-24s $settings%-26s validation benign F1 $metric%.4f${if (metric == best) "  <- chosen" else ""}")
        }
      case _ =>
    }

    val experiments = Seq(
      ("standard", data.filter(col("split") === "train"), data.filter(col("split") === "test")),
      ("cross_day", data.filter(col("capture_day") === "2019-01-12"), data.filter(col("capture_day") === "2019-03-11"))
    )

    experiments.foreach { case (experiment, trainRows, test) =>
      println(s"\n=== Experiment: $experiment")
      val train = balanced(trainRows)

      detectors.foreach { case (name, stage) =>
        val model = fit(stage, train)
        recordTuning(experiment, name, model)
        results.evaluate(experiment, "detection", name, model, test, "is_attack", detectionClasses)

        if (experiment == "standard") {
          model.write.overwrite().save(s"/models/detection/$name")
          if (name == "random_forest") {
            val forest = model.stages(0).asInstanceOf[TrainValidationSplitModel].bestModel
              .asInstanceOf[RandomForestClassificationModel]
            BuildFeatures.features.zip(forest.featureImportances.toArray).sortBy(-_._2).toDF("feature", "importance")
              .coalesce(1).write.mode("overwrite").parquet("/data/reports/feature_importance")
          }
        }
      }

      if (experiment == "standard") {
        val family = fit(new RandomForestClassifier()
          .setLabelCol("family_index").setNumTrees(100).setMaxDepth(12).setSeed(7), train)
        family.write.overwrite().save("/models/family/random_forest")
        results.evaluate(experiment, "family", "random_forest", family, test, "family_index", families)
      }

      train.unpersist()
    }

    results.save("")
    tuning.toDF().coalesce(1).write.mode("overwrite").parquet("/data/reports/tuning")

    spark.stop()
  }
}
