import pyspark.sql.functions as f
from pyspark.sql.functions import when
from pyspark.sql import SparkSession

def is_major_client(df_client, name_col):
    return df_client.filter(df_client[name_col]>=18)

def join_client_and_city(df_client,df_city,name_col):
    return df_client.join(df_city, df_client[name_col] == df_city[name_col], "inner").drop(df_city[name_col])

def add_department(df):
    return  df.withColumn(
        "departement",
        when(f.col("zip").startswith("20"), when(f.col("zip") <= "20190", "2A").otherwise("2B"))
        .otherwise(f.expr("substring(zip, 0, 2)"))
    )


