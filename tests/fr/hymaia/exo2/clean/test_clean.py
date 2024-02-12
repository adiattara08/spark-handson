from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.clean.clean import is_major_client,join_client_and_city,add_department
from src.fr.hymaia.exo2.agregate.aggregate import agg
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, LongType

class CleanTest(unittest.TestCase):
    def test_is_major_client(self):

        #given
        df = spark.createDataFrame(
            [
                ('Latch',67,'17540'),
                ('Whitson',24,'61570'),
                ('Graham', 65, '30350'),
                ('Taylor', 10, '91400'),
                ('Taylor', 9, '91400'),
                ('Bolt', 60, '11270'),
                ('Zabinski', 45, '68150'),
                ('Sweeney', 6, '75009'),
                ('Young', 43, '32110'),
                ('Stephens', 54, '70320'),
                ('Merritt', 8, '08250'),
                ('Cadena', 14, '22300')

            ],
            ['name','age','zip']
        )
        df_temoin =  spark.createDataFrame(
            [
                ('Latch',67,'17540'),
                ('Whitson',24,'61570'),
                ('Graham', 65, '30350'),
                ('Bolt', 60, '11270'),
                ('Zabinski', 45, '68150'),
                ('Young', 43, '32110'),
                ('Stephens', 54, '70320'),

            ],
            ['name','age','zip']
        )
        #when
        df_result = is_major_client(df,"age")

        #then
        self.assertEqual(df_temoin.schema, df_result.schema)
        self.assertEqual(df_result.collect(), df_temoin.collect())


    def test_join_client_and_city(self):

        #given
        df_client = spark.createDataFrame(
            [
                ('Zabinski', 45, '68150'),
                ('Young', 43, '32110'),
                ('Stephens', 54, '70320'),
            ],
            ['name', 'age', 'zip']
        )
        df_city = spark.createDataFrame(
            [
                ('70320', 'VILLE DU PONT'),
                ('32110', 'VILLERS GRELOT'),
                ('68150', 'SOLAURE EN DIOIS'),
                ('68150', 'ALEYRAC'),
            ],
            ['zip', 'city']
        )
        expected_result = spark.createDataFrame(
            [
                ('Stephens', 54, '70320', 'VILLE DU PONT'),
                ('Young', 43, '32110', 'VILLERS GRELOT'),
                ('Zabinski', 45, '68150', 'SOLAURE EN DIOIS'),
                ('Zabinski', 45, '68150', 'ALEYRAC')

            ],
            ['name', 'age', 'zip', 'city']
        )
        #when
        df_result = join_client_and_city(df_client, df_city,"zip")

        #then
        self.assertEqual(df_result.schema,expected_result.schema)
        self.assertEqual(df_result.collect(),expected_result.collect())

    def test_add_department(self):

        #given
        df= spark.createDataFrame(
            [
                ('Zabinski', 45, '68150', 'SOLAURE EN DIOIS'),
                ('Young', 43, '32110', 'VILLERS GRELOT'),
                ('Stephens', 54, '70320', 'VILLE DU PONT'),
                ('Stephens', 54, '68150', 'ALEYRAC'),
                ('Goodwin',35, '20190', 'DAKAR'),
                ('Marshall', 90, '20260', 'BAMAKO'),
            ],
            ['name', 'age', 'zip', 'city']
        )

        expected_result = spark.createDataFrame(
            [
                ('Zabinski', 45, '68150', 'SOLAURE EN DIOIS', '68'),
                ('Young', 43, '32110', 'VILLERS GRELOT', '32'),
                ('Stephens', 54, '70320', 'VILLE DU PONT', '70'),
                ('Stephens', 54, '68150', 'ALEYRAC', '68'),
                ('Goodwin', 35, '20190', 'DAKAR', '2A'),
                ('Marshall', 90, '20260', 'BAMAKO', '2B'),
            ],
            ['name', 'age', 'zip', 'city', 'departement']
        )

        #when
        df_result = add_department(df)
        self.assertEqual(df_result.schema,expected_result.schema)
        self.assertEqual(df_result.collect(), expected_result.collect())






