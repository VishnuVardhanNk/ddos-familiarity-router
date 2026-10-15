package ddos.test

import ddos.common.{Postgres, Session}
import org.apache.spark.ml.feature.PCA
import org.apache.spark.ml.functions.vector_to_array
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object GateMap {
  def main(args: Array[String]): Unit = {
    val spark = Session("GateMap", 24)
    import spark.implicits._

    val gate = Gate.fit(spark)
    val limit = gate.limits(0.99)
    val day1 = spark.read.parquet("/data/features")
      .filter(col("capture_day") === "2019-01-12" && col("split") === "test").select("label_family", "features")
    val scored = Gate.scoredDay2(spark)
    val day2 = scored.select("label_family", "occurrences", "features")

    val pca = new PCA().setInputCol("gate_scaled").setOutputCol("flat").setK(2)
      .fit(gate.scaled(day1.orderBy(rand(7)).limit(25000)
        .unionByName(day2.drop("occurrences").orderBy(rand(7)).limit(25000))))
    def flattened(rows: DataFrame): DataFrame =
      pca.transform(rows).withColumn("x", vector_to_array(col("flat"))(0)).withColumn("y", vector_to_array(col("flat"))(1))
    def located(rows: DataFrame): DataFrame =
      flattened(gate.scaled(gate.measure(rows))).withColumn("unfamiliar", col("distance") > limit)

    val centres = flattened(gate.centres.zipWithIndex.map { case (centre, cluster) => (cluster, centre) }.toDF("cluster", "gate_scaled"))
      .select(col("cluster"), col("x").as("centre_x"), col("y").as("centre_y")).persist()

    val points = located(day1.orderBy(rand(7)).limit(2500)).withColumn("day", lit("day 1"))
      .unionByName(located(day2.drop("occurrences").orderBy(rand(7)).limit(3000)).withColumn("day", lit("day 2")))
      .select("day", "label_family", "cluster", "x", "y", "distance", "unfamiliar")

    val mostCommon = Window.partitionBy("label_family").orderBy(desc("occurrences"))
    val flatDistance = sqrt(pow(col("x") - col("centre_x"), 2) + pow(col("y") - col("centre_y"), 2))
    val clearest = Window.partitionBy("label_family", "unfamiliar").orderBy(abs(flatDistance - col("distance")))
    val examples = located(day2.withColumn("place", row_number().over(mostCommon)).filter(col("place") <= 200))
      .join(centres, "cluster")
      .withColumn("pick", row_number().over(clearest)).filter(col("pick") === 1)
      .select(concat(col("label_family"), when(col("unfamiliar"), " connection, unfamiliar").otherwise(" connection, familiar")).as("name"),
        col("label_family"), col("cluster"),
        col("x"), col("y"), col("centre_x"), col("centre_y"), col("distance"), lit(limit).as("threshold"), col("unfamiliar"))

    val families = scored.groupBy("label_family").agg(
      sum("occurrences").as("flows"),
      (sum(when(col("unfamiliar"), col("occurrences")).otherwise(0L)) * 100.0 / sum("occurrences")).as("unfamiliar_pct"))

    Postgres.write(points, "gate_points")
    Postgres.write(centres, "gate_centres")
    Postgres.write(examples, "gate_examples")
    Postgres.write(families, "gate_families")
    println(f"RESULT GateMap written, threshold $limit%.3f")

    spark.stop()
  }
}
