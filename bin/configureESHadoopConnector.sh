#!/bin/bash

NUM_CORES=*

ES_CONNECTOR="../../elasticsearch-hadoop-8.0.0/dist"


export SPARK_HOME="/opt/spark/"
export PATH=$PATH:$SPARK_HOME/bin



$SPARK_HOME/bin/spark-submit --master spark://zabelin-Latitude-5420:7077  --driver-memory 8g \
    --jars $ES_CONNECTOR/elasticsearch-hadoop-8.0.0.jar ../lib/spark-jobs/test.py

