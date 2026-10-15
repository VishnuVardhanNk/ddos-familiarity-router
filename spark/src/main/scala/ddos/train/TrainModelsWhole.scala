package ddos.train

import ddos.common.Session
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification._
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.tuning.TrainValidationSplitModel
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object TrainModelsWhole {
  private def chosen[M](name: String): M =
    PipelineModel.load(s"/models/detection/$name").stages(0)
      .asInstanceOf[TrainValidationSplitModel].bestModel.asInstanceOf[M]

  def weighted(rows: DataFrame): DataFrame = {
    val counts = rows.groupBy("is_attack").count().collect().map(r => r.getDouble(0) -> r.getLong(1)).toMap
    val nBenign = counts(0.0)
    val nAttack = counts(1.0)
    val total = (nBenign + nAttack).toDouble
    println(f"  training on all $nBenign%,d benign + $nAttack%,d attack rows")
    rows.withColumn("weight", when(col("is_attack") === 0.0, total / (2 * nBenign)).otherwise(total / (2 * nAttack)))
      .persist(StorageLevel.MEMORY_AND_DISK)
  }

  def main(args: Array[String]): Unit = {
    val spark = Session("TrainModelsWhole", 96)

    val data = spark.read.parquet("/data/features")
    val families = PipelineModel.load("/models/encoder").stages(0).asInstanceOf[StringIndexerModel].labelsArray(0).toSeq
    val detectionClasses = Seq("benign", "attack")
    val results = new Evaluation(spark)

    val forest = chosen[RandomForestClassificationModel]("random_forest")
    val boosted = chosen[GBTClassificationModel]("gradient_boosted_trees")
    println(s"Settings chosen by tuning: random forest maxDepth=${forest.getMaxDepth}, numTrees=${forest.getNumTrees}; " +
      s"gradient-boosted trees maxDepth=${boosted.getMaxDepth}, maxIter=${boosted.getMaxIter}")

    val detectors = TrainModels.detectors.map {
      case ("random_forest", _) => "random_forest" -> new RandomForestClassifier()
        .setLabelCol("is_attack").setWeightCol("weight").setSubsamplingRate(0.8).setSeed(7)
        .setMaxDepth(forest.getMaxDepth).setNumTrees(forest.getNumTrees)
      case ("gradient_boosted_trees", _) => "gradient_boosted_trees" -> new GBTClassifier()
        .setLabelCol("is_attack").setWeightCol("weight").setSeed(7)
        .setMaxDepth(boosted.getMaxDepth).setMaxIter(boosted.getMaxIter)
      case other => other
    }

    val experiments = Seq(
      ("standard", data.filter(col("split") === "train"), data.filter(col("split") === "test")),
      ("cross_day", data.filter(col("capture_day") === "2019-01-12"), data.filter(col("capture_day") === "2019-03-11"))
    )

    experiments.foreach { case (experiment, trainRows, test) =>
      println(s"\n=== Experiment: $experiment (all rows)")
      val train = weighted(trainRows)

      detectors.foreach { case (name, stage) =>
        val model = TrainModels.fit(stage, train)
        results.evaluate(experiment, "detection", name, model, test, "is_attack", detectionClasses)
        model.write.overwrite().save(s"/models/whole/$experiment/detection/$name")
      }

      if (experiment == "standard") {
        val family = TrainModels.fit(new RandomForestClassifier()
          .setLabelCol("family_index").setNumTrees(100).setMaxDepth(12).setSeed(7), train)
        family.write.overwrite().save("/models/whole/standard/family/random_forest")
        results.evaluate(experiment, "family", "random_forest", family, test, "family_index", families)
      }

      train.unpersist()
      results.save("whole_")
    }

    spark.stop()
  }
}
