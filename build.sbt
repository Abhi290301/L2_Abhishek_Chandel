ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.3.2",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.3.2",
  "io.delta" %% "delta-core" % "2.2.0",
  "org.apache.spark" % "spark-avro_2.13" % "3.3.2",
  "org.postgresql" % "postgresql" % "42.5.4"

)
lazy val root = (project in file("."))
  .settings(
    name := "GharWalaKaam"
  )
