import configparser
import pyspark

def get_spark_ap_config() -> pyspark.SparkConf:
    spark_conf = pyspark.SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for k, v in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(k, v)
    return spark_conf

def load_survey_df(spark, data_file) -> pyspark.sql.DataFrame:
    return spark.read. \
        option("header", "true"). \
        option("inferSchema", "true"). \
        csv(data_file)

def count_by_country(survey_df) -> pyspark.sql.DataFrame:
    return survey_df\
        .where("Age < 40")\
        .select("Age", "Gender", "Country", "state")\
        .groupBy("Country")\
        .count()