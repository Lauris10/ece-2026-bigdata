#!/bin/bash
# Usage: 08.iceberg-introduction/run.sh <script name without .py>
# Runs a script in a PySpark session in the background, with a small memory footprint.
cd /home/onyxia/work
echo "exec(open('/home/onyxia/work/08.iceberg-introduction/$1.py').read())" > /tmp/stdin_$1.py
nohup pyspark --master "local[1]" --driver-memory 500m --driver-java-options "-XX:+UseSerialGC -XX:MaxMetaspaceSize=320m -XX:ReservedCodeCacheSize=48m" --conf spark.ui.enabled=false --conf spark.sql.shuffle.partitions=2 --conf spark.jars.ivy=/home/onyxia/work/.ivy2 --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 < /tmp/stdin_$1.py > /tmp/$1.log 2>&1 &
