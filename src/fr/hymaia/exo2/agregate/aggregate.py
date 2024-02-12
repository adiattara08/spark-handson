import pyspark.sql.functions as f
from pyspark.sql.functions import when
from pyspark.sql import SparkSession

def agg(df,):
    df_agg = df.groupBy("departement").agg(f.countDistinct("name","age","zip").alias("nb_people"))
    return df_agg.orderBy(f.desc("nb_people"), "departement")