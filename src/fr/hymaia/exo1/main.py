import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main():
    spark = (SparkSession.
             builder.
             appName("wordcount").
             master("local[*]").
             getOrCreate())
    # read csv file
    # attention header true si non la colonne text va être considérer
    df = spark.read.option("header", "true").option("inferSchema", "true").csv("src/resources/exo1/data.csv")

    df_wordcount = wordcount(df, "text")

    #ajouter le mode overwrite sino la deuxième exécution ne marchera pas
    df_wordcount.write.partitionBy("count").mode("overwrite").parquet("data/exo1/output")

def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()\

