# Ingests transactions dataset from GCS into BigQuery raw layer.
# Three modes:
#   reference   - one-time full load of users or cards dimension tables
#   bootstrap   - one-time historical baseline load for transactions 1991-2018
#   incremental - single-day load called daily by the Airflow DAG

import os
import tempfile

import click
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery, storage

load_dotenv()

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCS_RAW_BUCKET")
RAW_DATASET = "raw"

BOOTSTRAP_CUTOFF_YEAR = 2018

GCS_PATHS = {
    "users": "users/sd254_users.csv",
    "cards": "cards/sd254_cards.csv",
    "transactions": "transactions/credit_card_transactions-ibm_v2.csv",
}


def _download_from_gcs(blob_name: str, local_path: str) -> None:
    """Download a file from GCS to a local path."""
    client = storage.Client()
    client.bucket(BUCKET_NAME).blob(blob_name).download_to_filename(local_path)
    click.echo(f"Downloaded gs://{BUCKET_NAME}/{blob_name}")


def _load_to_bq(
    df: pd.DataFrame,
    table_id: str,
    write_disposition: str,
    time_partitioning: bigquery.TimePartitioning | None = None,
) -> None:
    """Load a DataFrame into a BigQuery table."""
    client = bigquery.Client(project=PROJECT_ID)
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    if time_partitioning:
        job_config.time_partitioning = time_partitioning
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()


def _build_transaction_date(chunk: pd.DataFrame) -> pd.DataFrame:
    """Construct a transaction_date column from separate Year, Month, Day columns."""
    chunk["transaction_date"] = pd.to_datetime(
        chunk[["Year", "Month", "Day"]].rename(
            columns={"Year": "year", "Month": "month", "Day": "day"}
        )
    )
    return chunk


def _sanitize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns to BigQuery-safe names: lowercase, alphanumeric and underscores only.

    BigQuery rejects column names containing special characters such as '?' or spaces.
    Applies to all source files — e.g. 'Is Fraud?' becomes 'is_fraud',
    'Errors?' becomes 'errors', 'Use Chip' becomes 'use_chip'.
    """
    df.columns = (
        df.columns.str.lower()
        .str.replace(r"[^a-z0-9_]", "_", regex=True)
        .str.replace(r"_+", "_", regex=True)
        .str.strip("_")
    )
    return df


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--source-file",
    type=click.Choice(["users", "cards"]),
    required=True,
    help="Reference file to load: 'users' or 'cards'",
)
def reference(source_file: str) -> None:
    """One-time full load of users or cards reference data into the raw layer.

    Users and cards are dimension tables managed separately from transaction
    processing — they are loaded once on pipeline launch, not on a daily schedule.
    """
    table_id = f"{PROJECT_ID}.{RAW_DATASET}.{source_file}"
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, f"{source_file}.csv")
        _download_from_gcs(GCS_PATHS[source_file], path)
        df = pd.read_csv(path)
        if source_file == "users":
            # sd254_users.csv has no explicit ID column — the join key to cards
            # and transactions is the row index position in the source file.
            # We expose it explicitly here so the column is queryable in BigQuery.
            df.insert(0, "user_id", df.index)
        df = _sanitize_columns(df)
        _load_to_bq(df, table_id, "WRITE_TRUNCATE")
    click.echo(f"Loaded {len(df):,} rows into {table_id}")


@cli.command()
@click.option("--chunksize", default=500_000, type=int, help="Rows per chunk")
def bootstrap(chunksize: int) -> None:
    """One-time historical baseline load for transactions 1991-2018.

    Bootstrap load is a standard pattern when launching a new pipeline against
    a pre-existing data source: bulk-load the historical baseline in one operation,
    then hand off to the daily incremental scheduler going forward.
    """
    table_id = f"{PROJECT_ID}.{RAW_DATASET}.transactions"
    # MONTH partitioning avoids BigQuery's 4,000-partition-per-load-job limit.
    # DAY partitioning across 1991-2018 would produce ~10,000 partitions, exceeding it.
    partitioning = bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.MONTH,
        field="transaction_date",
    )
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "transactions.csv")
        _download_from_gcs(GCS_PATHS["transactions"], path)
        total_rows = 0
        first_load = True  # track first non-empty chunk to control WRITE_TRUNCATE
        for i, chunk in enumerate(pd.read_csv(path, chunksize=chunksize, iterator=True)):
            chunk = chunk[chunk["Year"] <= BOOTSTRAP_CUTOFF_YEAR]
            if chunk.empty:
                continue
            chunk = _build_transaction_date(chunk)
            chunk = _sanitize_columns(chunk)
            _load_to_bq(
                chunk,
                table_id,
                write_disposition="WRITE_TRUNCATE" if first_load else "WRITE_APPEND",
                time_partitioning=partitioning if first_load else None,
            )
            first_load = False
            total_rows += len(chunk)
            click.echo(f"Chunk {i + 1}: {len(chunk):,} rows loaded (total: {total_rows:,})")
    click.echo(f"Bootstrap complete. Loaded {total_rows:,} rows into {table_id}")


@cli.command()
@click.option(
    "--date",
    "execution_date",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Execution date in YYYY-MM-DD format — passed from Airflow {{ ds }}",
)
@click.option("--chunksize", default=500_000, type=int, help="Rows per chunk")
def incremental(execution_date, chunksize: int) -> None:
    """Load a single day's transactions — called daily by the Airflow DAG.

    Filters the source file to rows matching the execution date and appends
    them to raw.transactions. Note: running twice for the same date will
    produce duplicates — deduplication is handled downstream in the dbt
    staging model.
    """
    year, month, day = execution_date.year, execution_date.month, execution_date.day
    table_id = f"{PROJECT_ID}.{RAW_DATASET}.transactions"
    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "transactions.csv")
        _download_from_gcs(GCS_PATHS["transactions"], path)
        total_rows = 0
        for i, chunk in enumerate(pd.read_csv(path, chunksize=chunksize, iterator=True)):
            chunk = chunk[
                (chunk["Year"] == year)
                & (chunk["Month"] == month)
                & (chunk["Day"] == day)
            ]
            if chunk.empty:
                continue
            chunk = _build_transaction_date(chunk)
            chunk = _sanitize_columns(chunk)
            _load_to_bq(chunk, table_id, "WRITE_APPEND")
            total_rows += len(chunk)
    click.echo(
        f"Incremental load complete. Loaded {total_rows:,} rows "
        f"for {execution_date.date()} into {table_id}"
    )


if __name__ == "__main__":
    cli()
