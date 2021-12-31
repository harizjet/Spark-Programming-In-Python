import unittest
from pyspark.sql import SparkSession
from lib.utils import get_spark_ap_config, load_survey_df, count_by_country

class UtilsTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession\
            .builder\
            .config(conf=get_spark_ap_config())\
            .getOrCreate()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_datafile_loading(self):
        sample_df = load_survey_df(self.spark, "data/sample.csv")
        result_count = sample_df.count()
        self.assertEqual(result_count, 203, "Record count should be 203")

    def test_country_count(self):
        sample_df = load_survey_df(self.spark, "data/sample.csv")
        count_list = count_by_country(sample_df).collect()
        result = {k: v for k, v in count_list}
        self.assertEqual(result["United States"], 88, "Count for United States should be 88")
        self.assertEqual(result["United Kingdom"], 23, "Count for United Kingdom should be 23")
        self.assertEqual(result["Canada"], 46, "Count for Canada should be 46")

if __name__ == "__main__":
    unittest.main()