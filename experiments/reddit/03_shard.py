# This is a pysparksql job
# ==> could be done by Tuplex as soon as JSON is supported
import time

import glob
import argparse
import os
import sqlite3
import pandas as pd
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

spark = SparkSession.builder.config("spark.hadoop.validateOutputSpecs", "false").appName("RedditSharding").getOrCreate()

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

# create db output path!
os.mkdir(os.path.join(args.dest_path, 'db'))

spark.read.json(args.data_path, schema=schema).limit(20).createOrReplaceTempView('comments')
rdd = spark.sql("SELECT * FROM comments ORDER BY author").rdd.map(lambda r: (r['author'], r)) \
    .groupByKey().sortByKey().zipWithIndex()


# rdd has now structure of: ((author, iterable), idx)

# db_name is a string,
# rows should be list of dictionaries
# check this https://stackoverflow.com/questions/466521/how-many-files-can-i-put-in-a-directory
# we use ext4, so should be fine...
def save_to_sqlite(db_name, rows):
    db_path = os.path.join(args.dest_path, 'db', db_name) + '.sqlite3'

    # delete file if it exists
    if os.path.exists(db_path):
        os.remove(db_path)

    conn = sqlite3.connect(db_path)
    conn.execute('''CREATE TABLE comments (author text, author_flair_css_class text, author_flair_text text,
    body text, can_gild text, controversiality integer, created_utc integer, distinguished text, edited integer,
     gilded integer, id text, is_submitter integer, link_id text, parent_id text, permalink text, retrieved_on integer,
      score integer, stickied integer, subreddit text, subreddit_id text, subreddit_type text)''')
    conn.commit()

    prepared_rows = list(map(lambda t: (t['author'], t['author_flair_css_class'], t['author_flair_text'],
                                        t['body'], t['can_gild'], t['controversiality'], t['created_utc'],
                                        t['distinguished'],
                                        t['edited'],
                                        t['gilded'], t['id'], t['is_submitter'], t['link_id'], t['parent_id'],
                                        t['permalink'], t['retrieved_on'],

                                        t['score'], t['stickied'], t['subreddit'], t['subreddit_id'],
                                        t['subreddit_type']), rows))

    conn.executemany('INSERT INTO comments VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', prepared_rows)
    conn.commit()
    conn.close()
    return (rows[0]['author'], len(rows), db_name, db_path)


# Note: PysparkSQL doesn't support following expression:
# lambda t: (t[0][0], t[1], map(lambda x: x.asDict(), list(t[0][1])))
# this gives a pickling error -.-
def reorg_udf(t):
    L = []
    for item in list(t[0][1]):
        L.append(item.asDict())

    return (t[0][0], t[1], L)

res = rdd.map(reorg_udf).map(lambda t: save_to_sqlite('u{}'.format(t[1]), t[2])).collect()
df = pd.DataFrame(res, columns=['author', 'rows', 'dbname', 'sqlite_path'])

# save user/sqlite db lookup table as csv
lookup_path = os.path.join(args.dest_path, "user_table.csv")
df.to_csv(lookup_path, index=None)
# rdd.map(lambda t: (t[0][0], t[1])).map(lambda t: '{},{}'.format(t[0], t[1])).saveAsTextFile(lookup_path)

print('>> done')
# spark.sql("SELECT DISTINCT author FROM comments").show()
# spark.sql("SELECT COUNT(*) AS num_comments FROM comments GROUP BY author").show()
