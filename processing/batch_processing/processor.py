import json
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType, BooleanType, DataType, TimestampType
from datetime import datetime, timedelta
from pyspark.sql import Window

from processing.schema import schema


BOOTSTRAP_SERVERS = "172.31.65.164:9092,172.31.67.180:9092,172.31.67.236:9092"
STATES_PATH = "/home/ubuntu/data/USstate.json"


def get_topics(arr):
    return [e["topic_name"] for e in arr]


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("MeetupsBatchProcessing") \
        .getOrCreate()

    states = spark \
        .read \
        .format("json") \
        .load(STATES_PATH) \
        .withColumnRenamed("name", "state_name")

    events = spark \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", "meetups") \
        .option("startingOffsets", "earliest") \
        .load()

    events = events \
        .withColumn("value", F.col("value").cast(StringType())) \
        .withColumn("value", F.from_json("value", schema)) \
        .select("value.*") \
        .withColumn("time", F.to_timestamp(F.from_unixtime(F.col("mtime") / 1000))) \
        .withColumn("group_topics", F.udf(get_topics, ArrayType(StringType()))("group.group_topics"))

    now = datetime.now()
    delta = timedelta(minutes=now.minute, seconds=now.second, microseconds=now.microsecond)
    upper = now - delta
    lower1 = upper - timedelta(hours=6)
    lower2 = upper - timedelta(hours=3)
    lower3 = upper - timedelta(hours=6)

    query1 = events \
        .where((events["time"] > lower1) & (events["time"] <= upper)) \
        .groupby("group.group_country") \
        .agg(F.count("*").alias("count"))

    query2 = events \
        .where((events["time"] > lower2) & (events["time"] < upper)) \
        .where(F.col("group.group_country") == "us") \
        .join(states, F.col("group.group_state") == states["code"])
    query2 = query2 \
        .groupby("state_name") \
        .agg(F.collect_set("group.group_name").alias("groups"))

    query3 = events \
        .where((events["time"] > lower3) & (events["time"] < upper)) \
        .withColumn("topic", F.explode("group_topics")) \
        .groupby("group.group_country", "topic") \
        .agg(F.count("*").alias("count")) \
        .withColumn("rank", F.row_number().over(Window.partitionBy("group_country").orderBy(F.desc("count")))) \
        .where(F.col("rank") == 1).drop("rank")

    query1_result = {"time_start": lower1.hour, "time_end": upper.hour,
                     "statistics": [{row["group_country"]: row["count"]} for row in query1.collect()]}
    query2_result = {"time_start": lower2.hour, "time_end": upper.hour,
                     "statistics": [{row["state_name"]: row["groups"]} for row in query2.collect()]}
    query3_result = {"time_start": lower3.hour, "time_end": upper.hour,
                     "statistics": [{row["group_country"]: {row["topic"]: row["count"]}} for row in query3.collect()]}

    s3 = boto3.resource("s3")
    bucket = s3.Bucket("andrii-prysiazhnyk-bucket")

    bucket.put_object(Key="project/query1.json", Body=json.dumps(query1_result))
    bucket.put_object(Key="project/query2.json", Body=json.dumps(query2_result))
    bucket.put_object(Key="project/query3.json", Body=json.dumps(query3_result))
