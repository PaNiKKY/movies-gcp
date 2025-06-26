from airflow.decorators import dag, task, task_group
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import (
    ClusterGenerator,
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import asyncio
import json
import os
from dotenv import load_dotenv
from datetime import datetime
import sys
import os
load_dotenv()

from etl.movies_extraction import get_movie_ids, run_box_office_data, run_movie_credits, run_movie_details


project_id = os.getenv("project_id")
bucket_suffix = os.getenv("bucket_suffix")
region = os.getenv("region")

bucket_name = f"{project_id}-{bucket_suffix}"
bq_temp = f"{project_id}-bq-temp"
cluster_name = f"{project_id}-cluster"

with open("/home/airflow/gcs/dags/upsert.sql", "r") as u:
    query_string = u.read()

@dag(
    schedule_interval="@daily",
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['gcp', 'etl']
)
def movie_pipeline_dag():

    date = "{{ ds }}"
    # date = "2025-06-21"

    cluster_generator_config = ClusterGenerator(
        project_id=project_id,
        region=region,
        cluster_name=cluster_name,
        num_masters=1,
        master_machine_type='e2-standard-2',
        master_disk_size=100,
        num_workers=4,
        worker_machine_type='e2-standard-4',
        worker_disk_size=200,
        image_version='2.2-debian12',
        subnetwork_uri='default',
        internal_ip_only=True
    ).make()

    CLEANING_JOB = {
        "reference": {"project_id": project_id},
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {"main_python_file_uri": "gs://sodium-keel-461511-u2-movies-scripts/dags/etl/clean_typing.py",
                        "args": [
                                    "--date_input", date,
                                    "--bucket_name", bucket_name
                                ]
                        },
        }
    MODEL_JOB = {
        "reference": {"project_id": project_id},
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {"main_python_file_uri": "gs://sodium-keel-461511-u2-movies-scripts/dags/etl/models.py",
                        "args": [
                                    "--date_input", date,
                                    "--bucket_name", bucket_name,
                                    "--dataset", f"{project_id}.movies_dataset",
                                    "--bq_temp", bq_temp
                                ]
                        },
        }

    _create_bucket_task = GCSCreateBucketOperator(
        task_id='create_bucket_if_not_exists',
        bucket_name=bucket_name,
        project_id=project_id,
        location=region.split("-")[0].upper()
        )

    @task_group(group_id='extraction')
    def extraction_group(date):
        @task
        def download_movie_ids(date):
            movie_ids = get_movie_ids(date)
            print(f"movie_ids: {len(movie_ids)}")
            return movie_ids

        @task
        def extract_movie_details_task(movie_id, date):
            movie_details= run_movie_details(movie_id)
            imdb_ids = [movie['imdb_id'] for movie in movie_details if movie['imdb_id']]
            year, month, day = date.split("-")
            gcs_hook = GCSHook()
            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=f'raw/{year}/{month}/{day}/movie_details.json',
                data=json.dumps(movie_details, indent=4),
                mime_type='text/json'
            )
            print(f"movie_details: {len(movie_details)}\nimdb_ids: {len(imdb_ids)}")
            return imdb_ids

        @task
        def extract_box_office_data_task(movie_id, date):
            box_office_data = run_box_office_data(movie_id)

            year, month, day = date.split("-")
            gcs_hook = GCSHook()
            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=f'raw/{year}/{month}/{day}/box_office.json',
                data=json.dumps(box_office_data, indent=4),
                mime_type='text/json'
            )
            print(f"movie_details: {len(box_office_data)}\nimdb_ids: {len(movie_id)}")

        @task
        def extract_movie_credits_task(movie_id,date):
            movie_credits = run_movie_credits(movie_id)
            year, month, day = date.split("-")
            gcs_hook = GCSHook()
            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=f'raw/{year}/{month}/{day}/movie_credits.json',
                data=json.dumps(movie_credits, indent=4),
                mime_type='text/json'
            )
            print(f"movie_details: {len(movie_credits)}\nimdb_ids: {len(movie_id)}")

        movie_ids = download_movie_ids(date)
        imdb_ids = extract_movie_details_task(movie_ids,date)
        extract_box_office_data_task(imdb_ids,date)
        extract_movie_credits_task(movie_ids,date)

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=project_id,
        cluster_config=cluster_generator_config,
        region=region,
        cluster_name=cluster_name
    )

    transform_job_task = DataprocSubmitJobOperator(
        task_id="transform_job_task", 
        job=CLEANING_JOB, 
        region=region, 
        project_id=project_id
    )

    modeling_job_task = DataprocSubmitJobOperator(
        task_id="modeling_job_task", 
        job=MODEL_JOB, 
        region=region, 
        project_id=project_id
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=project_id,
        cluster_name=cluster_name,
        region=region,
        trigger_rule="none_failed_min_one_success"
    )

    upsert_to_fact_table = BigQueryInsertJobOperator(
        task_id='upsert_to_fact_table',
        configuration={
                "query": {
                    "query":query_string.format(
                        project_id=project_id,
                        bigqury_dataset="movies_dataset",
                        date=date
                    ),
                    "useLegacySql": False,
                }
            }
        )
    
    delete_staging_table = BigQueryInsertJobOperator(
        task_id='delete_staging_table',
        configuration={
                "query": {
                    "query":
                        f"""
                            DROP TABLE IF EXISTS `{project_id}.movies_dataset.staging_movies`;
                            DROP TABLE IF EXISTS `{project_id}.movies_dataset.staging_production_companies`;
                            DROP TABLE IF EXISTS `{project_id}.movies_dataset.staging_credits`;
                        """,
                    "useLegacySql": False,
                }
            }
        )

    _create_bucket_task >> [extraction_group(date), create_cluster] >> transform_job_task
    transform_job_task >> modeling_job_task >> [delete_cluster, upsert_to_fact_table]
    upsert_to_fact_table >> delete_staging_table

movie_pipeline_dag()
