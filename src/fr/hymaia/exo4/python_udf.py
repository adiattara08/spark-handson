import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("exo4").master("local[*]").getOrCreate()

def main():

    # read csv file
    df = spark.read.option("header", "true").csv("src/resources/exo4/sell.csv")
    df_cat = add_category_name(df)
    df_cat.show()



def add_category_name(df):
    return df.withColumn('category_name', extract_title_udf(df.category))

@f.udf('string')
def extract_title_udf(cat_id):
    return "food" if int(cat_id) <6 else "furniture"
