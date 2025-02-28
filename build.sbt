name := "flink-playground"

version := "0.1"

scalaVersion := "2.12.20"

val flinkVersion    = "1.20.1"
val postgresVersion = "42.2.2"
val logbackVersion  = "1.2.10"

fork := true
javaOptions += "--add-opens=java.base/java.util=ALL-UNNAMED"

libraryDependencies ++= Seq(
  "org.apache.flink"  % "flink-clients"             % flinkVersion,
  "org.apache.flink" %% "flink-scala"               % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala"     % flinkVersion,
  "org.apache.flink"  % "flink-connector-kafka"     % "3.2.0-1.19",
  "org.apache.flink" %% "flink-connector-cassandra" % "3.2.0-1.19",
  "org.apache.flink"  % "flink-connector-jdbc"      % "3.2.0-1.19",
  "org.postgresql"    % "postgresql"                % postgresVersion,
  "ch.qos.logback"    % "logback-core"              % logbackVersion,
  "ch.qos.logback"    % "logback-classic"           % logbackVersion
)

run / fork := true
run / javaOptions += "--add-opens=java.base/java.util=ALL-UNNAMED"
