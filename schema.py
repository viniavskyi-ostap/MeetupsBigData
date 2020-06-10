from pyspark.sql.types import (StructType, StructField, StringType, \
                               ArrayType, LongType, DoubleType)

schema = StructType([
    StructField("venue", StructType([
        StructField("venue_name", StringType(), True),
        StructField("lon", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("venue_id", DoubleType(), True),

    ]), True),
    StructField("visibility", StringType(), True),
    StructField("response", StringType(), True),
    StructField("guests", LongType(), True),
    StructField("member", StructType([
        StructField("member_id", LongType(), True),
        StructField("photo", StringType(), True),
        StructField("member_name", StringType(), True),
    ]), True),
    StructField("rsvp_id", LongType(), True),
    StructField("mtime", LongType(), True),
    StructField("event", StructType([
        StructField("event_name", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("time", LongType(), True),
        StructField("event_url", StringType(), True),
    ]), True),
    StructField("group", StructType([
        StructField("group_topics", ArrayType(StructType([
            StructField("urlkey", StringType(), True),
            StructField("topic_name", StringType(), True),
        ])), True),
        StructField("group_city", StringType(), True),
        StructField("group_country", StringType(), True),
        StructField("group_id", LongType(), True),
        StructField("group_name", StringType(), True),
        StructField("group_lon", DoubleType(), True),
        StructField("group_lat", DoubleType(), True),
        StructField("group_urlname", StringType(), True),
        StructField("group_state", StringType(), True),
    ]), True),
])
