from pyspark.sql import SparkSession
from lib.utils import get_spark_ap_config, load_survey_df, count_by_country
from lib.logger import Log4J
import sys
import time

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .config(conf=get_spark_ap_config())\
        .getOrCreate()

    logger = Log4J(spark)

    if len(sys.argv) != 2:
        logger.error("Not input file")
        sys.ext(-1)

    logger.info("Starting HelloSpark")

    df = load_survey_df(spark, sys.argv[1])
    df = df.repartition(3)
    df = count_by_country(df)
    logger.info(df.collect())

    logger.info("Ending HelloSpark")
    time.sleep(120)
    spark.stop()

