# ЧТЕНИЕ СТРИМА
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime
from pyspark.sql.types import (
    StructField,
    StringType,
    StructType,
    LongType
)


spark_jars_packages = ("org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0")

spark = SparkSession.builder \
    .appName('Kafka-read-test') \
    .config('spark.jars.packages', spark_jars_packages) \
    .getOrCreate()

kafka_df = spark.readStream \
    .format("kafka") \
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
    .option('kafka.security.protocol', 'SASL_SSL') \
    .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";') \
    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
    .option('subscribe', 'student.topic.cohort14.s27040058') \
    .option('startingOffsets', 'earliest') \
    .load()

incoming_message_schema = StructType(
    [
        StructField("restaurant_id", StringType(), True),
        StructField("adv_campaign_id", StringType(), True),
        StructField("adv_campaign_content", StringType(), True),
        StructField("adv_campaign_owner", StringType(), True),
        StructField("adv_campaign_owner_contact", StringType(), True),
        StructField("adv_campaign_datetime_start", LongType(), True),
        StructField("adv_campaign_datetime_end", LongType(), True),
        StructField("datetime_created", LongType(), True),
    ]
)

parsed_df = kafka_df.select(
    from_json(
        col("value").cast(StringType()),
        incoming_message_schema
    ).alias("parsed_value")
)

result_df = parsed_df.select(
    col("parsed_value.restaurant_id"),
    col("parsed_value.adv_campaign_id"),
    col("parsed_value.adv_campaign_content"),
    col("parsed_value.adv_campaign_owner"),
    col("parsed_value.adv_campaign_owner_contact"),
    from_unixtime(
        col("parsed_value.adv_campaign_datetime_start")
        ).cast("timestamp").alias('adv_campaign_datetime_start'),
    from_unixtime(
            col("parsed_value.adv_campaign_datetime_end")
            ).cast("timestamp").alias('adv_campaign_datetime_end'),
    from_unixtime(
            col("parsed_value.datetime_created")
            ).cast("timestamp").alias('datetime_created'),
)


result_df.writeStream \
    .format('console') \
    .outputMode('append') \
    .start() \
    .awaitTermination()
