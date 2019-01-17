from elasticsearch import Elasticsearch
es = Elasticsearch()

from pyspark.ml.recommendation import ALSModel

from pyspark.sql.types import *
from pyspark.sql.functions import udf, lit, current_timestamp, unix_timestamp
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

sc = SparkContext('local')
spark = SparkSession(sc)


def convert_vector(x):
    return " ".join(["%s|%s" % (i, v) for i, v in enumerate(x)])

def vector_to_struct(x, version, ts):
    return (convert_vector(x), version, ts)

vector_struct = udf(vector_to_struct, \
                    StructType([StructField("factor", StringType(), True), \
                                StructField("version", StringType(), True),\
                                StructField("timestamp", LongType(), True)]))
    
model = ALSModel.load("model")

ver = model.uid
ts = unix_timestamp(current_timestamp())

model.itemFactors.select("id", vector_struct("features", lit(ver), ts).alias("@model")).write.format("es") \
    .option("es.mapping.id", "id") \
    .option("es.write.operation", "update") \
    .save("demo/movies", mode="append")

model.userFactors.select("id", vector_struct("features", lit(ver), ts).alias("@model")).write.format("es") \
    .option("es.mapping.id", "id") \
    .option("es.write.operation", "index") \
    .save("demo/users", mode="append")
