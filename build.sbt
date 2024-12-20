ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

val flinkVersion = "1.13.2"
val postgresVersion = "42.2.2"
val logbackVersion = "1.2.10"

lazy val root = (project in file("."))
  .settings(
    name := "flink-playground",
    libraryDependencies := Seq(
      "org.apache.flink" %% "flink-clients" % flinkVersion,
      "org.apache.flink" %% "flink-scala" % flinkVersion,
      "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
      "org.apache.flink" %% "flink-connector-kafka" % flinkVersion,
      "org.apache.flink" %% "flink-connector-cassandra" % flinkVersion,
      "org.apache.flink" %% "flink-connector-jdbc" % flinkVersion,
      "org.postgresql" % "postgresql" % postgresVersion,
      "ch.qos.logback" % "logback-core" % logbackVersion,
      "ch.qos.logback" % "logback-classic" % logbackVersion
    )
  )