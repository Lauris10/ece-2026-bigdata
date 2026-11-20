#!/bin/bash
cd /home/onyxia/work
echo "exec(open('/home/onyxia/work/07.spark-dataframes/$1.py').read())" > 07.spark-dataframes/stdin_$1.py
nohup pyspark --master "local[1]" --driver-memory 500m --driver-java-options "-XX:+UseSerialGC -XX:MaxMetaspaceSize=320m -XX:ReservedCodeCacheSize=48m" --conf spark.ui.enabled=true --conf spark.sql.shuffle.partitions=2 --conf spark.jars.ivy=/home/onyxia/work/.ivy2 --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 < 07.spark-dataframes/stdin_$1.py > 07.spark-dataframes/$1.log 2>&1 &
