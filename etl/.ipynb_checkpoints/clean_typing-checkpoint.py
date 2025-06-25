from pyspark.sql.functions import *
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    FloatType, ArrayType, DateType, LongType,
    BooleanType
)
from pyspark.sql import SparkSession
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--date_input", type=str)
parser.add_argument("--bucket_name", type=str)

args = parser.parse_args()

bucket_name = args.bucket_name
date = args.date_input
year, month, day = date.split("-")

bucket_temp = "sodium-keel-461511-u2-spark-temp"

spark = SparkSession.builder\
        .appName("transform-1")\
        .getOrCreate()

spark.conf.set('temporaryGcsBucket', bucket_temp)

def read_json_file(file_name):
    print(f"gs://{bucket_name}/raw/{year}/{month}/{day}/{file_name}.json")
    df = spark.read \
                .option("mode", "PERMISSIVE") \
                .option("multiline","true") \
                .json(f"gs://{bucket_name}/raw/{year}/{month}/{day}/{file_name}.json")
    return df

def save_to_parquet(df, file_name):
    df.write \
        .mode("overwrite") \
        .parquet(f"gs://{bucket_name}/clean/{year}/{month}/{day}/{file_name}.parquet")


# movie details clean and typing
movie_details_df = read_json_file("movie_details")
movie_details_clean = movie_details_df \
                    .select(col("id").cast(IntegerType()), 
                            "imdb_id", 
                            "original_title", 
                            "title", 
                            col("budget").cast(IntegerType()), 
                            transform(col("genres"), lambda x: struct(
                                                                        x["id"].cast(IntegerType()).alias("genre_id"),
                                                                        x["name"].alias("genre_name")
                                                                     )).alias("genres"), 
                            "origin_country",
                            col("popularity").cast(FloatType()),
                            transform(col("production_companies"), lambda x: struct(
                                                                                    x["id"].cast(IntegerType()).alias("company_id"),
                                                                                    x["name"].alias("company_name"),
                                                                                    x["origin_country"].alias("company_country")
                                                                                )).alias("production_companies"), 
                            col("release_date").cast(DateType()),
                            "revenue", 
                            col("runtime").cast(IntegerType()), 
                            col("vote_average").cast(FloatType()), 
                            col("vote_count").cast(IntegerType())) \
                    .dropna(subset=["id","popularity"]) \
                    .dropDuplicates(["id"]) \
                    .withColumnRenamed("id", "movie_id")


# movie credits clean and typing
credit_df = read_json_file("movie_credits")
credit_clean = credit_df \
                .withColumn("movie_id", col("id").cast(IntegerType())) \
                .drop("id") \
                .withColumn(
                        "cast",
                        transform(col("cast"), lambda x: struct(x["id"].cast(IntegerType()).alias("person_id"), 
                                                                x["gender"].cast(IntegerType()).alias("gender"),
                                                                x["name"].alias("cast_name"),
                                                                x["original_name"].alias("cast_original_name"),
                                                                x["order"].cast(IntegerType()).alias("order"),
                                                                x["known_for_department"],
                                                                
                                                               )
                                 )
                    ) \
                .withColumn(
                        "crew",
                        transform(col("crew"), lambda x: struct(x["id"].cast(IntegerType()).alias("person_id"), 
                                                                x["gender"].cast(IntegerType()).alias("gender"),
                                                                x["name"].alias("crew_name"),
                                                                x["original_name"].alias("crew_original_name"),
                                                                x["department"],
                                                                x["job"]
                                                               )
                                 )
                    ) \
                .dropna(subset=["movie_id"]) \
                .dropDuplicates(["movie_id"])

crew_filter_df = credit_clean \
            .select(
                "movie_id",
                explode("crew").alias("crew")
            ) \
            .select(
                "movie_id",
                "crew.*"
            ) \
            .where((col("job") == "Director")| (col("job") == "Writer") | (col("job") == "Screenplay"))


cast_filter_df = credit_clean \
            .select(
                "movie_id",
                explode("cast").alias("cast")
            ) \
            .select(
                "movie_id",
                "cast.*"
            ) \
            .where(col("order") <= 20)


# box office clean and typing
box_office_df = read_json_file("box_office")
box_office_clean = box_office_df \
                    .withColumn("worldwide_gross", regexp_replace(col("worldwide_gross"), "\\$|,", "").cast(LongType())) \
                    .withColumn("domestic_opening", regexp_replace(col("domestic_opening"), ",", "").cast(IntegerType())) \
                    .withColumn("budget", regexp_replace(col("budget"), ",", "").cast(IntegerType())) \
                    .dropna(subset=["imdb_id"]) \
                    .dropDuplicates(["imdb_id"])

movie_details_clean.printSchema()
crew_filter_df.printSchema()
cast_filter_df.printSchema()
box_office_clean.printSchema()

# save to parquet
save_to_parquet(movie_details_clean,"movie_details")
save_to_parquet(crew_filter_df,"crew_of_movies")
save_to_parquet(cast_filter_df,"cast_of_movies")
save_to_parquet(box_office_clean,"box_office.parquet")