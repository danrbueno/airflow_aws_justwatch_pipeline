# DAG tasks:
# 1. Extract titles from endpoint 'https://apis.justwatch.com/graphql' and save the raw data into JSON files.
# 2. Upload JSON files to AWS S3 bucket.
# 3. Trigger AWS Glue Jobs that process the titles data to normalized data into parquets.
#    Example: 1 title has many production countries in the raw data. 
#             The Glue Job separates all these productions countries from raw data 
#             and transform them into parquets that will be read from a Redshift database.
import json
import logging
import os
from datetime import datetime

import pandas as pd
import requests
from airflow import DAG
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.utils.task_group import TaskGroup

# Airflow connection ID set under Airflow environment.
# You can set it under Admin/Connections interface.
AWS_CONN_ID = "AWSConnection"

# Variables set under Airflow environment.
# You can set them under Admin/Variables interface.
STAGING_DIR = Variable.get("bucket_staging_dir")
BUCKET_NAME = Variable.get("bucket_name")
AWS_REGION_NAME = Variable.get("aws_region_name")

LOG_FORMATTER = logging.Formatter(
    "%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s"
)
CONSOLE_HANDLER = logging.StreamHandler()
CONSOLE_HANDLER.setFormatter(LOG_FORMATTER)

LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
LOGGER.addHandler(CONSOLE_HANDLER)

DEFAULT_ARGS = {
    "owner": "justwatch",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
}

CLUSTER_YEARS = [1899, 1950, 1980, 1990, 2000, 2010, 2012, 2014, 2016, 2018, 2020, 2023]


# Send a request to the endpoint to get all the titles.
def get_titles(post_data, cursor: str = "", titles: list = None, start: bool = True):
    # The API uses only one endpoint to get the queries. So, we need just change the parameters
    # and the query to collect the streamings data.
    url_endpoint = "https://apis.justwatch.com/graphql"

    # The website prevents a robot to get their data without specifying a User-Agent.
    # Let's create our request headers!
    headers = {
        "content-type": "application/json",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/101.0.4951.41 Safari/537.36 Edg/101.0.1210.32",
        "accept-encoding": "gzip, deflate, br",
    }

    if not titles:
        titles = []

    if cursor and not start:
        post_data["variables"]["popularAfterCursor"] = cursor
    else:
        post_data["variables"]["popularAfterCursor"] = ""

    req = requests.post(url_endpoint, data=json.dumps(post_data), headers=headers)
    if req.status_code != 200:
        errors = json.loads(req.text)["errors"]

        for error in errors:
            print(
                str(error["extensions"])
                + " : "
                + str(error["locations"])
                + " : "
                + error["message"]
            )
            raise requests.ConnectionError("!!!ERRORS!!!")

    results = req.json()["data"]["popularTitles"]
    titles.extend(results["edges"])

    # Recursive function due to pagination limits.
    # So the function will be executed until the response achive the last page.
    if results["pageInfo"]["hasNextPage"]:
        cursor = results["pageInfo"]["endCursor"]
        get_titles(post_data=post_data, cursor=cursor, titles=titles, start=False)

    return titles


# Save the titles into JSON files, clustered by a range of years specified in the filters.
def save_cluster(data, filter_range):
    titles = [d["node"] for d in data]
    min = filter_range["min"]
    max = filter_range["max"]
    file_name = f"dags/{STAGING_DIR}/titles_{min}_{max}.json"
    
    titles_df = pd.json_normalize(data=titles, sep="_", max_level=5)
    titles_df.reset_index(drop=True, inplace=True)
    titles_df.to_json(file_name, orient="records", lines=True, indent=4)


# Start the extract task, sending request to endpoint and save the results into JSON files,
# clustered by a range of years specified in the filters.
def extract_data(filter_range):
    LOGGER.info(
        "Retrieving titles with release year between {min} and {max}...".format(
            min=filter_range["min"], max=filter_range["max"]
        )
    )

    # The post data and the query are stored in the following files:
    # - postData.json
    # - query.graphql
    # Let's open** those files and saves to a variable to use it.
    with open("dags/src/postData.json", "r", encoding="utf-8") as file:
        post_data = json.load(file)

    with open("dags/src/query.graphql", "r", encoding="utf-8") as file:
        query = file.read()

    # To get the data we need the reference code of the streaming to pass as a GraphQL variable.
    # I put the most watrched streaming services reference codes into a JSON file 
    # which will be read and associated to the variable as a list.
    # To see all the streaming service codes available in JustWatch, see dags/src/all_streaming_services.csv.
    df_streamings = pd.read_json("dags/src/streaming_services.json", lines=True)
    packages = [streaming.code for streaming in df_streamings.itertuples()]

    post_data["query"] = query
    post_data["variables"]["popularTitlesFilter"]["packages"] = packages
    post_data["variables"]["offerFilter"]["packages"] = packages

    raw = []

    post_data["variables"]["popularTitlesFilter"]["releaseYear"] = filter_range
    cluster_titles = get_titles(post_data=post_data)
    raw.extend(cluster_titles)
    save_cluster(data=cluster_titles, filter_range=filter_range)


# Upload the staging files to AWS S3 bucket
def upload_files():
    hook = S3Hook(AWS_CONN_ID)

    for filename in os.listdir(f"dags/{STAGING_DIR}"):
        hook.load_file(
            filename=f"dags/{STAGING_DIR}/{filename}",
            key=f"{STAGING_DIR}/{filename}",
            bucket_name=BUCKET_NAME,
            replace=True,
        )


# Trigger AWS Glue Job, based in a job name saved in AWS Glue Jobs.
def trigger_glue_job(job_name):
    session = AwsGenericHook(aws_conn_id=AWS_CONN_ID)

    # Get a client in the same region as the Glue job
    boto3_session = session.get_session(
        region_name=AWS_REGION_NAME,
    )

    # Trigger the job using its name
    client = boto3_session.client("glue")
    client.start_job_run(
        JobName=job_name,
    )

with DAG(
    "aws_process_justwatch_data",
    default_args=DEFAULT_ARGS,
    description="Process JustWatch titles, upload to S3 bucket and trigger glue jobs to save parquets files.",
    tags=["justwatch", "aws", "S3", "Glue Jobs"],
    catchup=False,
):
    start = EmptyOperator(task_id="start_dag")

    # Define a group of tasks based in the years in the CLUSTER_YEARS.
    # Each range of year will determine a task to be executed one by one.
    with TaskGroup("extract_tasks") as task_group_extract:
        cluster_tasks = []

        for i in range(len(CLUSTER_YEARS) - 1):
            min = CLUSTER_YEARS[i] + 1
            max = CLUSTER_YEARS[i + 1]
            filter_range = {"min": min, "max": max}

            task_extract_data = PythonOperator(
                task_id=f"extract_data_{min}_{max}",
                python_callable=extract_data,
                op_kwargs={"filter_range": filter_range},
            )

            cluster_tasks.append(task_extract_data)

        for i in range(len(cluster_tasks) - 1):
            cluster_tasks[i].set_downstream(cluster_tasks[i + 1])

    # Task to upload the files to AWS S3 after the end of extraction task group.
    task_upload_files = PythonOperator(
        task_id="upload_to_s3", python_callable=upload_files
    )

    # Task to trigger the AWS Glue Jobs to be executed after the upload to S3 bucket.
    with TaskGroup("glue_job_tasks") as task_group_trigger_glue_jobs:
        glue_jobs = [
            "job_save_titles",
            "job_save_genres",
            "job_save_streaming_services",
            "job_save_titles_production_countries",
            "job_save_titles_streaming_services",
            "job_save_titles_genres",
        ]

        task_glue_job = [
            PythonOperator(
                task_id=f"trigger_glue_{job_name}",
                python_callable=trigger_glue_job,
                op_kwargs={"job_name": job_name},
            )
            for job_name in glue_jobs
        ]

    end = EmptyOperator(task_id="end_dag")

    (
        start
        >> task_group_extract
        >> task_upload_files
        >> task_group_trigger_glue_jobs
        >> end
    )
