from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.functions import lit
import json
import numpy as np

conf = SparkConf().setAppName("updateTest")
sc = SparkContext(conf=conf)
sc.setLogLevel("INFO")
spark = SQLContext(sc)

def constructQuery(page_name):
    #TODO: will not work for more than a year data, must be reworked
    start = 0
    q = '{ "from" : ' + str(start) + ', "size" : 10000, "query": { "query_string": { "query": "Page.keyword: \\"' + page_name + '\\"", "default_field": "Page" } } }'
    return q

page = "Java"

es_read_conf = {
    # specify the node that we are sending data to (this should be the master)
    "es.nodes" : 'localhost',
    # specify the p ort in case it is not the default port
    "es.port" : '9200',
    # specify a resource in the form 'index/doc-type'
    "es.resource" : 'log-wiki-31',
    "es.scroll.size" : "10000",
    "es.query" : constructQuery(page),
}

es_rdd = sc.newAPIHadoopRDD(
    inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
    keyClass="org.apache.hadoop.io.NullWritable",
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
    conf=es_read_conf)

data = es_rdd.values().sortBy(lambda log: log["Date"])
visits = np.array(data.map(lambda log: log["Visits"]).collect(), dtype=int)
times = data.map(lambda log: log["Date"]).collect()
print(visits[:10])
print(times[:10])

