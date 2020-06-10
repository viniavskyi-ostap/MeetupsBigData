from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from datetime import datetime
from processing.schema import schema


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

df = (spark.
      readStream.
      format("kafka").
      option("kafka.bootstrap.servers", "localhost:9092").
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

q1 = write_to_cassandra(df1, keyspace='meetups', table='event_cities', trigger_time='1 minute')
q2 = write_to_cassandra(df2, keyspace='meetups', table='events', trigger_time='1 minute')
q3 = write_to_cassandra(df3, keyspace='meetups', table='cities_groups', trigger_time='1 minute')
q4 = write_to_cassandra(df4, keyspace='meetups', table='groups_events', trigger_time='1 minute')

q1.start()
q2.start()
q3.start()
q4.start()

q1.awaitTermination()
q2.awaitTermination()
q3.awaitTermination()
q4.awaitTermination()
