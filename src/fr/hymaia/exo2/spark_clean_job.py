
from pyspark.sql import SparkSession
from .clean import clean as cl


def main():
    ## Création spark Session
    spark = (SparkSession.
             builder.
             appName("clean").
             master("local[*]").
             getOrCreate())

    ## Lecture des fichiers
    df_city = spark.read.option("header", "true").csv("src/resources/exo2/city_zipcode.csv")
    df_client= spark.read.option("header", "true").csv("src/resources/exo2/clients_bdd.csv")

    df_join = get_adult_and_city(df_client, df_city)
    #add department
    df_department = cl.add_department(df_join)

    #Ecriture
    df_department.write.mode("overwrite").parquet("data/exo2/output")


def get_adult_and_city(people_df, city_df):
    adult_df = cl.is_major_client(people_df, "age")
    join_df = cl.join_client_and_city(adult_df, city_df, "zip")
    return join_df

