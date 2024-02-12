from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.clean.clean import is_major_client,join_client_and_city,add_department
from src.fr.hymaia.exo2.agregate.aggregate import agg
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType
import pyspark



class AggregateTest(unittest.TestCase):
    def test_agg(self):
        #given
        df = spark.createDataFrame(
            [
                ('Zabinski', 45, '68150', 'SOLAURE EN DIOIS', '68'),
                ('Young', 43, '32110', 'VILLERS GRELOT', '32'),
                ('Stephens', 54, '70320', 'VILLE DU PONT', '70'),
                ('Stephens', 54, '68150', 'ALEYRAC', '68'),
                ('Amadou', 30, '20190', 'DAKAR', '2A'),
                ('Sarah', 30, '68150', 'CITY', '68'),
            ],
            ['name', 'age', 'zip', 'city', 'departement']
        )

        expected_result_schema = StructType([
            StructField('departement', StringType(), True),
            StructField('nb_people', LongType(), False)
        ])
        expected_result = spark.createDataFrame(
            [
                ('68', 3),
                ('2A', 1),
                ('32', 1),
                ('70', 1),
            ],
            expected_result_schema
        )

        #when
        df_result = agg(df)

        #then
        self.assertEqual(expected_result.schema, df_result.schema)
        self.assertEqual(df_result.collect(), expected_result.collect())

    def test_agg_with_invalid_data(self):
        #given
        df = spark.createDataFrame(
            [
                ('Zabinski', 45, '68150', 'SOLAURE EN DIOIS', '68'),
                ('Young', 43, '32110', 'VILLERS GRELOT', '32'),
                ('Stephens', 54, '70320', 'VILLE DU PONT', '70'),
                ('Stephens', 54, '68150', 'ALEYRAC', '68'),
                ('Amadou', 30, '20190', 'DAKAR', '2A'),
                ('Sarah', 30, '68150', 'CITY', '68'),

            ],
            ['name', 'age', 'zip', 'city', 'departement']
        )

        with self.assertRaises(Exception) as context:
            # when
            agg_result = agg(df.select("name", "age"))

        self.assertEqual(type(context.exception), pyspark.sql.utils.AnalysisException)
