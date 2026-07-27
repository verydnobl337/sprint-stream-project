# ЧТЕНИЕ СТАТИЧНЫХ ДАННЫХ
from pyspark.sql import SparkSession


spark_jars_packages = ("org.postgresql:postgresql:42.4.0")

spark = SparkSession.builder \
    .appName("Postgres-read-test") \
    .config('spark.jars.packages', spark_jars_packages) \
    .getOrCreate()

postgresql_setting = {
    'user': 'student',
    'password': 'de-student'
}

subscribers_restaurant_df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de") \
    .option("driver", "org.postgresql.Driver") \
    .option("dbtable", "subscribers_restaurants") \
    .options(**postgresql_setting) \
    .load()

subscribers_restaurant_df.show()