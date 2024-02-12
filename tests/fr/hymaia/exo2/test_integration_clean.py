from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.spark_clean_job import get_adult_and_city
from src.fr.hymaia.exo2.agregate.aggregate import agg
from pyspark.sql import Row


class CleanJobTest(unittest.TestCase):

    def test_integration_clean(self):


         city_df = spark.createDataFrame(
            [
                ("10001", "New York"),
                ("90001", "Los Angeles"),
            ],
            ['zip', 'city'])
         people_df = spark.createDataFrame(
            [
                ("Turing", 21, "10001"),
                ("Gates", 12, "10001"),
                ("Hopper", 42, "90001"),
            ],
            ['name', 'age', 'zip'])
         expected_result = spark.createDataFrame(
                [
                    ("Turing", 21, "10001", "New York"),
                    ("Hopper", 42, "90001", "Los Angeles"),
                ],
                ['name', 'age', 'zip', 'city'])
        # when
         result = get_adult_and_city(people_df, city_df)
        # then
         self.assertEqual(result.schema, expected_result.schema)
         self.assertEqual(result.collect(), expected_result.collect())
