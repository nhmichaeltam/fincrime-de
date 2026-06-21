# Loads a CSV file from GCS bucket into BigQuery raw table in chunks.

import os

import click
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery, storage

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCS_RAW_BUCKET")

# Download file from GCS to a local path for chunked reading.
def download_from_gcs(bucket_name: str, blob_name: str, local_path: str) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(local_path)
    click.echo(f"Downloaded gs://{bucket_name}/{blob_name} to {local_path}")


def load_csv_in_chunks(
    local_path: str,
    destination_table: str,
    chunksize: int,
) -> None:

# first chunk replaces the table (fresh load), subsequent chunks append.

    client = bigquery.Client()
    reader = pd.read_csv(local_path, chunksize=chunksize, iterator=True)

    total_rows = 0
    for i, chunk in enumerate(reader):
        write_disposition = "WRITE_TRUNCATE" if i == 0 else "WRITE_APPEND"
        job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)

        job = client.load_table_from_dataframe(
            chunk, destination_table, job_config=job_config
        )
        job.result()  # wait for the job to finish before loading the next chunk

        total_rows += len(chunk)
        click.echo(f"Chunk {i + 1}: loaded {len(chunk)} rows (total so far: {total_rows})")

    click.echo(f"Done. Loaded {total_rows} rows into {destination_table}")


@click.command()
@click.option(
    "--source-blob",
    required=True,
    help="Path to the CSV file inside the GCS bucket, e.g. paysim/raw/paysim_transactions.csv",
)
@click.option(
    "--local-path",
    default="data/raw_transactions_test.csv",
    help="Local path to download the file to before loading",
)
@click.option(
    "--target-table",
    default="transactions_staging_test",
    help="Destination BigQuery table name within the raw dataset",
)
@click.option(
    "--chunksize",
    default=500_000,
    type=int,
    help="Number of rows to read and load per chunk",
)
def run(source_blob: str, local_path: str, target_table: str, chunksize: int) -> None:
    destination_table = f"{PROJECT_ID}.raw.{target_table}"

    download_from_gcs(BUCKET_NAME, source_blob, local_path)
    load_csv_in_chunks(local_path, destination_table, chunksize)


if __name__ == "__main__":
    run()