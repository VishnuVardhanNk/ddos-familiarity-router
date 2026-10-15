package ddos.common

import org.apache.spark.sql.DataFrame

object Postgres {
  private val connection = Map(
    "url" -> "jdbc:postgresql://postgres:5432/ddos",
    "user" -> "ddos",
    "password" -> sys.env("POSTGRES_PASSWORD"),
    "driver" -> "org.postgresql.Driver"
  )

  def write(df: DataFrame, table: String, mode: String = "overwrite"): Unit =
    df.write.format("jdbc").options(connection).option("dbtable", table).mode(mode).save()
}
