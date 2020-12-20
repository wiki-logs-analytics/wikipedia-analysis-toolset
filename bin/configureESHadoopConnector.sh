#!/bin/bash

NUM_CORES=*

ES_CONNECTOR="/home/kirill/elasticsearch-hadoop-7.10.1/dist"


export SPARK_HOME="/opt/spark/"
export PATH=$PATH:$SPARK_HOME/bin



$SPARK_HOME/bin/spark-submit --master spark://kirill-Inspiron-15-7000-Gaming:7077 --driver-memory 8g \
    --jars $ES_CONNECTOR/elasticsearch-hadoop-7.10.1.jar ../lib/spark-jobs/test.py

