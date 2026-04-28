from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os
import logging

S3_BUCKET = "your-retailpulse-bucket"
S3_PREFIX = "retailpulse/landing/"
LOCAL_DATA_DIR = "/usr/local/airflow/include/data/raw"
FILES = ["customers.csv", "orders.csv", "events.csv"]

default_args = {
    "owner": "retailpulse",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


@dag(
    dag_id="retailpulse_ingestion",
    default_args=default_args,
    description="Upload raw e-commerce CSVs to S3 landing zone",
    schedule="@daily",
    start_date=datetime(2026, 4, 28),
    catchup=False,
    is_paused_upon_creation=True,
    tags=["retailpulse", "ingestion", "week1"],
)
def retailpulse_ingestion():

    @task()
    def upload_customers():
        return upload_file("customers.csv")

    @task()
    def upload_orders():
        return upload_file("orders.csv")

    @task()
    def upload_events():
        return upload_file("events.csv")

    @task()
    def verify_uploads(sizes: list):
        hook = S3Hook(aws_conn_id="aws_default")
        for filename in FILES:
            s3_key = f"{S3_PREFIX}{filename}"
            exists = hook.check_for_key(key=s3_key, bucket_name=S3_BUCKET)
            if not exists:
                raise ValueError(f"Verification failed — {filename} not found in S3")
            logging.info(f"Verified: s3://{S3_BUCKET}/{s3_key}")
        logging.info("All files verified in S3.")

    sizes = [upload_customers(), upload_orders(), upload_events()]
    verify_uploads(sizes)


def upload_file(filename: str) -> dict:
    local_path = os.path.join(LOCAL_DATA_DIR, filename)

    if not os.path.exists(local_path):
        raise FileNotFoundError(f"File not found: {local_path}")

    hook = S3Hook(aws_conn_id="aws_default")
    s3_key = f"{S3_PREFIX}{filename}"

    logging.info(f"Uploading {local_path} → s3://{S3_BUCKET}/{s3_key}")
    hook.load_file(
        filename=local_path,
        key=s3_key,
        bucket_name=S3_BUCKET,
        replace=True,
    )

    size_mb = round(os.path.getsize(local_path) / (1024 * 1024), 2)
    logging.info(f"Uploaded {filename} ({size_mb} MB)")
    return {"filename": filename, "size_mb": size_mb}


retailpulse_ingestion()