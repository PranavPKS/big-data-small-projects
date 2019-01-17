from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import desc, array_contains

spark = SparkSession.builder.appName("myApp") \
 .config("spark.mongodb.input.uri", "mongodb://student:student@ec2-54-210-44-189.compute-1.amazonaws.com/test.reviews") \
 .getOrCreate()

df = spark.read.format("com.mongodb.spark.sql").load()


df_sel = df[['asin','overall']]
df_half = df_sel.groupBy("asin").agg(F.mean('overall').alias("avg_ratings"), F.count('overall').alias("num_ratings")).filter("`num_ratings` >= 100").sort(desc('avg_ratings'))


spark1 = SparkSession.builder.appName("myApp") \
.config("spark.mongodb.input.uri", "mongodb://student:student@ec2-54-210-44-189.compute-1.amazonaws.com/test.metadata") \
.getOrCreate()

df_meta = spark1.read.format("com.mongodb.spark.sql").load()

df_sel2 = df_meta[['asin','title','categories']]

df_join = df_half.join(df_sel2,["asin"])
categs = ["CDs & Vinyl","Movies & TV", "Toys & Games", "Video Games"]

with open("output.txt", "w") as fo:
    for categ in categs:
        df_cat = df_join.where(array_contains("categories", categ)).sort(desc('avg_ratings')).limit(5).toPandas()
        df_cat = df_cat[df_cat['avg_ratings']==df_cat['avg_ratings'][0]].sort_values(by=['num_ratings'], ascending=False)
        for i in range(len(df_cat)):
            fo.write(categ+"\t" + str(df_cat['title'][i]) + "\t" + str(df_cat['num_ratings'][i]) + "\t" + str(df_cat['avg_ratings'][i]) + "\n")
