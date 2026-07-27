from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    to_json,
    col,
    lit,
    struct,
    from_unixtime,
    current_timestamp,
    unix_timestamp,
)
from pyspark.sql.types import StructType, StructField, StringType, LongType

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
        "org.postgresql:postgresql:42.4.0",
    ]
)

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = (
    SparkSession.builder.appName("RestaurantSubscribeStreamingService")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.jars.packages", spark_jars_packages)
    .getOrCreate()
)

# читаем из топика Kafka сообщения с акциями от ресторанов
restaurant_read_stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091")
    .option("kafka.security.protocol", "SASL_SSL")
    .option(
        "kafka.sasl.jaas.config",
        'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";',
    )
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
    .option("subscribe", "student.topic.cohort14.s27040058")
    .option("startingOffsets", "earliest")
    .load()
)

# определяем схему входного сообщения для json
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

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = (
    restaurant_read_stream_df.select(
        from_json(col("value").cast(StringType()), incoming_message_schema).alias(
            "parsed_value"
        )
    )
    .select(
        col("parsed_value.restaurant_id"),
        col("parsed_value.adv_campaign_id"),
        col("parsed_value.adv_campaign_content"),
        col("parsed_value.adv_campaign_owner"),
        col("parsed_value.adv_campaign_owner_contact"),
        from_unixtime(col("parsed_value.adv_campaign_datetime_start"))
        .cast("timestamp")
        .alias("adv_campaign_datetime_start"),
        from_unixtime(col("parsed_value.adv_campaign_datetime_end"))
        .cast("timestamp")
        .alias("adv_campaign_datetime_end"),
        from_unixtime(col("parsed_value.datetime_created"))
        .cast("timestamp")
        .alias("datetime_created"),
    )
    .filter(
        (col("adv_campaign_datetime_start") <= current_timestamp())
        & (col("adv_campaign_datetime_end") >= current_timestamp())
    )
)


# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = (
    spark.read.format("jdbc")
    .option(
        "url", "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de"
    )
    .option("driver", "org.postgresql.Driver")
    .option("dbtable", "subscribers_restaurants")
    .option("user", "student")
    .option("password", "de-student")
    .load()
    .select("client_id", "restaurant_id")
)

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = (
    filtered_read_stream_df.join(subscribers_restaurant_df, "restaurant_id", "inner")
    .withColumn("trigger_datetime_created", unix_timestamp(current_timestamp()))
    .withColumn("feedback", lit(None))
)

query = (
    result_df.writeStream.format("console")
    .outputMode("append")
    .option("truncate", False)
    .start()
)

query.awaitTermination()
