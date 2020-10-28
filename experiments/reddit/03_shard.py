# This is a pysparksql job
# ==> could be done by Tuplex as soon as JSON is supported
import time

import glob
import argparse
import pyspark
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
    BooleanType,
LongType
)

spark = SparkSession.builder.appName("RedditSharding").getOrCreate()

parser = argparse.ArgumentParser(description='Reddit data prep')
parser.add_argument('-i', '--input-pattern', type=str, dest='data_path', default='./data/*.json',
                    help='path where to read data from')
parser.add_argument('-o', '--output-path', type=str, dest='dest_path', default='./sharded',
                    help='path where to save data to')


# Data looks like:
# {"author":"RedneckPapist",
# "author_flair_css_class":null,
# "author_flair_text":null,
# "body":"Look at that schnozz",
# "can_gild":true,
# "controversiality":0,
# "created_utc":1512086400,
# "distinguished":null,
# "edited":false,
# "gilded":0,
# "id":"dql1dzp",
# "is_submitter":false,
# "link_id":"t3_7gqpou",
# "parent_id":"t3_7gqpou",
# "permalink":"/r/milliondollarextreme/comments/7gqpou/adulting_is_sooo_hard_x3/dql1dzp/",
# "retrieved_on":1514212661,
# "score":5,
# "stickied":false,
# "subreddit":"milliondollarextreme",
# "subreddit_id":"t5_2vsta",
# "subreddit_type":"public"}

schema = StructType([
StructField("author", StringType(), True),
StructField("author_flair_css_class", StringType(), True),
StructField("author_flair_text", StringType(), True),
StructField("body", StringType(), True),
StructField("can_gild", StringType(), True),
StructField("controversiality", IntegerType(), True),
StructField("created_utc", LongType(), True),
StructField("distinguished", StringType(), True),
StructField("edited", BooleanType(), True),
StructField("gilded", IntegerType(), True),
StructField("id", StringType(), True),
StructField("is_submitter", BooleanType(), True),
StructField("link_id", StringType(), True),
StructField("parent_id", StringType(), True),
StructField("permalink", StringType(), True),
StructField("retrieved_on", LongType(), True),
StructField("score", IntegerType(), True),
StructField("stickied", BooleanType(), True),
StructField("subreddit", StringType(), True),
StructField("subreddit_id", StringType(), True),
StructField("subreddit_type", StringType(), True)
])


args = parser.parse_args()
spark.read.json(args.data_path, schema=schema).createOrReplaceTempView('comments')
spark.sql("SELECT DISTINCT author FROM comments").show()
spark.sql("SELECT COUNT(*) AS num_comments FROM comments GROUPBY author").show()