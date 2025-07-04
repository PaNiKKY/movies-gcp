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
parser.add_argument("--dataset", type=str)
parser.add_argument("--bq_temp", type=str)

args = parser.parse_args()

bucket_name = args.bucket_name
date = args.date_input
dataset = args.dataset
year, month, day = date.split("-")
bq_temp = args.bq_temp


spark = SparkSession.builder\
        .appName("model-app")\
        .getOrCreate()

spark.conf.set('temporaryGcsBucket', bq_temp)
spark.conf.set('intermediateFormat', "parquet")
spark.conf.set('enableListInference', "true")

def read_parquet_file(file_name):
    df = spark.read \
                .option("mode", "PERMISSIVE") \
                .parquet(f"gs://{bucket_name}/clean/{year}/{month}/{day}/{file_name}.parquet")
    return df

def write_to_bigquery(df, table_name):
    df.write \
        .mode("overwrite") \
        .format("bigquery") \
        .option("table", f"{dataset}.staging_{table_name}") \
        .save()

# Read the Parquet files form GCS
movie_details_df = read_parquet_file("movie_details")
cast_df = read_parquet_file("cast_of_movies")
crew_df = read_parquet_file("crew_of_movies")
box_office_df = read_parquet_file("box_office")

#Separate movies and production companies
movie_details_sep = movie_details_df \
    .withColumn(
        "genres",
        transform(col("genres"), lambda x: x["genre_name"])
    ) \
    .withColumn(
        "production_companies",
        transform(col("production_companies"), lambda x: x["company_id"])
    )

companies_df = movie_details_df \
    .select(
        explode(col("production_companies")).alias("production_companies")
    ) \
    .select(
        "production_companies.*"
    ) \
    .dropDuplicates(["company_id"])


cast_movie_df = cast_df \
    .groupBy(
        col("movie_id")
    ).agg(
        collect_list(
            struct(
                col("person_id"),
                col("order")
            )
        ).alias("casts")
    )

cast_details_df = cast_df \
    .select(
        "person_id",
        "name",
        "gender",
        "department"
    ) \
    .dropDuplicates(["person_id"])

crew_movie_id = crew_df \
    .groupBy(
        col("movie_id")
    ).agg(
        collect_list(
            struct(
                col("person_id"),
                col("job")
            )
        ).alias("crews")
    )

crew_details_df = crew_df \
    .select(
        "person_id",
        "name",
        "gender",
        "department"
    ) \
    .dropDuplicates(["person_id"])

box_office_df = box_office_df \
    .select(
        "imdb_id",
        col("budget").cast(LongType()),
        col("domestic_opening").cast(LongType()),
        col("worldwide_gross").cast(LongType()),
    )

# joining to movie_details
movie_join_df = movie_details_sep \
    .join(cast_movie_df, movie_details_sep.movie_id == cast_movie_df.movie_id, "left") \
    .join(crew_movie_id, movie_details_sep.movie_id == crew_movie_id.movie_id, "left") \
    .join(box_office_df, movie_details_sep.imdb_id == box_office_df.imdb_id, "left") \
    .withColumn(
        "opening_gross",
        col("domestic_opening").cast(LongType())
    ) \
    .withColumn(
        "movie_budget",
        coalesce(box_office_df.budget, movie_details_sep.budget).cast(LongType())
    ) \
    .withColumn(
        "metrics",
        array(
            struct(
                col("popularity").cast(FloatType()).alias("popularity"),
                col("vote_count").cast(IntegerType()).alias("vote_count"),  
                col("vote_average").cast(FloatType()).alias("vote_average"),
                coalesce(box_office_df.worldwide_gross, movie_details_sep.revenue).cast(LongType()).alias("gross"),
                lit(date).cast(DateType()).alias("collect_date")
              )
        )
    )  \
    .drop(crew_movie_id.movie_id, cast_movie_df.movie_id, box_office_df.imdb_id, 
          "budget", box_office_df.worldwide_gross, movie_details_sep.revenue, "domestic_opening")

# Union credit details
credit_df = cast_details_df.union(crew_details_df).dropDuplicates(["person_id"])

#print dataframes schema
movie_join_df.printSchema()
credit_df.printSchema()
companies_df.printSchema()

# Write the final DataFrame to bigquery temporary table
write_to_bigquery(movie_join_df, "movies")
write_to_bigquery(credit_df, "credits")
write_to_bigquery(companies_df, "production_companies")