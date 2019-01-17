from elasticsearch import Elasticsearch
es = Elasticsearch()

from pyspark.sql.functions import udf
from pyspark.sql.types import *
import re

#import pprint
#pp = pprint.PrettyPrinter(indent=4)

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col
sc = SparkContext('local')
spark = SparkSession(sc)

ratings = spark.read.csv("ml-latest/ratings.csv", header=True, inferSchema=True)

ratings.cache()
print("Number of ratings: " + str(ratings.count()))
print("Sample of ratings:")
ratings.show(5)

raw_movies = spark.read.csv("ml-latest/movies.csv", header=True, inferSchema=True)
print("Raw movie data:")
raw_movies.show(5)

extract_genres = udf(lambda x: x.lower().split("|"), ArrayType(StringType()))

raw_movies.select("movieId", "title", extract_genres("genres").alias("genres")).show(5,False)

def extract_year_fn(title):
    p = re.compile("\(\d{4}\)")
    res = p.search(title)
    try:
        if res:
            return (title[:res.start()-1],res.group()[1:-1])
        else:
            return (title, 1970)
    except:
        print(title)

extract_year = udf(extract_year_fn,StructType([StructField("title", StringType(), True),StructField("release_date", StringType(), True)]))

movies = raw_movies.select("movieId", extract_year("title").title.alias("title"),\
    extract_year("title").release_date.alias("release_date"),\
    extract_genres("genres").alias("genres"))
print("Cleaned movie data:")
movies.show(5)

links = spark.read.csv("ml-latest/links.csv", header=True, inferSchema=True)
movie_data = movies.join(links, movies.movieId == links.movieId).select(movies.movieId, movies.title, movies.release_date, movies.genres, links.tmdbId)

print("Cleaned movie data with tmdbId links:")
movie_data.show(5)

#es.indices.delete(index="demo")

create_index = {
  "settings": {
      "analysis": {
          "analyzer": {
              # this configures the custom analyzer we need to parse vectors such that the scoring
              "payload_analyzer": {
                                    "type": "custom",
                                    "tokenizer":"whitespace",
                                    "filter":"delimited_payload_filter"
                                  }
                        }
              }
          },
  "mappings": {
      "ratings": {
        "properties": {
                        "userId": {"type": "integer"},
                        "movieId": {"type": "integer"},
                        "rating": {"type": "double"},
                        "timestamp": {"type": "date"}
                      }  
                  },
    "users": {
        "properties": {
                        "userId": {"type": "integer"},
                        "@model": {
                            # this mapping definition sets up the fields for user factor vectors of our model
                            "properties": {
                                            "factor": {"type": "text",
                                                        "term_vector": "with_positions_offsets_payloads",
                                                        "analyzer" : "payload_analyzer"},
                                            "version": {"type": "keyword"},
                                            "timestamp": {"type": "date"}
                                          }
                                  }
                      }
            },
      "movies": {
          "properties": {
              "movieId":{ "type": "integer"},
              "tmdbId": {"type": "keyword"},
              "genres": {"type": "keyword"},
              "release_date": {"type": "date","format": "year"},
              "@model": {
                  # this mapping definition sets up the fields for movie factor vectors of our model
                  "properties": {
                                  "factor": {"type": "text",
                                             "term_vector": "with_positions_offsets_payloads",
                                             "analyzer" : "payload_analyzer"},
                                  "version": {"type": "keyword"},
                                  "timestamp": {"type": "date"}
                              }
                      }
                }
            }
  }
}



es.indices.create(index="demo", ignore=400, body=create_index)

ratings.write.format("es").save("demo/ratings")

movie_data.write.format("org.elasticsearch.spark.sql").option("es.mapping.id", "movieId").save("demo/movies")

ratings_from_es = spark.read.format("es").load("demo/ratings")
ratings_from_es.show(5)

als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", regParam=0.18, rank=13, maxIter=19, seed=12)
model = als.fit(ratings_from_es)
model.userFactors.show(5)
model.itemFactors.show(5)
model.save("model")
