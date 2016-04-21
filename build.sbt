/* build.sbt */
name := "Experiment-with-Spark-and-Update-Cassandra"
version := "1.0"
scalaVersion := "2.11.7"
libraryDependencies ++= Seq(
    "org.apache.spark"   %% "spark-core" % "1.5.0",
    "org.apache.spark"   %% "spark-sql"  % "1.5.0",
    "datastax" % "spark-cassandra-connector" % "1.5.0-RC1-s_2.11"
)
