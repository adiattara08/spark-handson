from .clean.clean import is_major_client,join_client_and_city,add_department
from .agregate import aggregate as ag
from pyspark.sql.functions import when
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import os
import shutil


def main():
    ## Création spark Session
    spark = (SparkSession.
             builder.
             appName("spark_job").
             master("local[*]").
             getOrCreate())

    ## Lecture des fichiers
    df_department = spark.read.parquet("data/exo2/output")

    #aggregation
    df_agg = ag.agg(df_department)

    df_agg.repartition(1).write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save("data/exo2/csv")


    create_single_csv_file("data/exo2/csv","data/exo2/")

def create_single_csv_file(source_repertoire, destination_repertoire):
    for fichier in os.listdir(source_repertoire):
        if fichier.lower().endswith('.csv'):
            source_fichier = os.path.join(source_repertoire, fichier)
            destination_fichier = os.path.join(destination_repertoire, "aggregate.csv")
            shutil.copy(source_fichier, destination_fichier)
    shutil.rmtree(source_repertoire)
