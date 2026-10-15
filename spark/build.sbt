ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "ddos-detection",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.5.5" % "provided",
      "org.apache.spark" %% "spark-mllib" % "3.5.5" % "provided",
      ("org.apache.spark" %% "spark-sql-kafka-0-10" % "3.5.5").excludeAll(ExclusionRule("org.apache.hadoop")),
      "org.postgresql" % "postgresql" % "42.7.4"
    ),
    assembly / assemblyJarName := "ddos-detection.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  )
