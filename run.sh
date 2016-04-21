#!/bin/bash
$SPARK_HOME/bin/spark-submit \
    --packages datastax:spark-cassandra-connector:1.5.0-RC1-s_2.11 \
    --class SparkUpdateCassandra \
    --master local \
    target/scala-2.11/experiment-with-spark-and-update-cassandra_2.11-1.0.jar
