package ddos.common

import org.apache.spark.sql.SparkSession

object Session {
  def apply(appName: String, shufflePartitions: Int = 24): SparkSession = {
    val spark = SparkSession.builder().appName(appName)
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .config("spark.sql.shuffle.partitions", shufflePartitions.toString)
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark
  }
}
