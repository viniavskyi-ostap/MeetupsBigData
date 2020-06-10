from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from datetime import datetime
from processing.schema import schema


BOOTSTRAP_SERVERS = "172.31.65.164:9092,172.31.67.180:9092,172.31.67.236:9092"


def write_to_cassandra(streaming_df, keyspace, table, trigger_time):
    query = (streaming_df.
             writeStream.
             format("org.apache.spark.sql.cassandra").
             option("table", table).
             option("keyspace", keyspace).
             trigger(processingTime=trigger_time)
             )

    return query


spark = SparkSession.builder \
    .appName(f"Meetups_{str(datetime.now())}") \
    .getOrCreate()

spark.conf.set("spark.sql.streaming.checkpointLocation", '.')
spark.conf.set("spark.cassandra.auth.username", "cassandra")
spark.conf.set("spark.cassandra.auth.password", "cassandra")


df = (spark.
      readStream.
      format("kafka").
      option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS).
      option("subscribe", "meetups").
      option("startingOffsets", "earliest").
      load())

df = (df.
      selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").
      withColumn('value', F.from_json('value', schema)).
      selectExpr('value.*'))

df1 = (df.
    select(
    F.col('group.group_country').alias('country'),
    F.col('group.group_city').alias('city')
))

df2 = (df.
    select(
    F.col('event.event_id').alias('event_id'),
    F.col('event.event_name').alias('event_name'),
    F.col('event.time').alias('event_time'),
    F.array_join('group.group_topics.topic_name', delimiter=';').alias('topics'),
    F.col('group.group_name').alias('group_name'),
    F.col('group.group_country').alias('country'),
    F.col('group.group_city').alias('city'),
))

df3 = (df.
    select(
    F.col('group.group_city').alias('city_name'),
    F.col('group.group_name').alias('group_name'),
    F.col('group.group_id').alias('group_id'),
))

df4 = (df.
    select(
    F.col('group.group_id').alias('group_id'),
    F.col('event.event_id').alias('event_id'),
    F.col('event.event_name').alias('event_name'),
    F.col('event.time').alias('event_time'),
    F.array_join('group.group_topics.topic_name', delimiter=';').alias('topics'),
    F.col('group.group_name').alias('group_name'),
    F.col('group.group_country').alias('country'),
    F.col('group.group_city').alias('city'),
))

q1 = write_to_cassandra(df1, keyspace='meetups', table='event_cities', trigger_time='1 minute').start()
q2 = write_to_cassandra(df2, keyspace='meetups', table='events', trigger_time='1 minute').start()
q3 = write_to_cassandra(df3, keyspace='meetups', table='cities_groups', trigger_time='1 minute').start()
q4 = write_to_cassandra(df4, keyspace='meetups', table='groups_events', trigger_time='1 minute').start()

q1.awaitTermination()
q2.awaitTermination()
q3.awaitTermination()
q4.awaitTermination()
