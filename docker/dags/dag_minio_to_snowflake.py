import os
import tempfile
import logging

from pathlib import Path
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


# ==========================================================
# Logging
# ==========================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# ==========================================================
# Configuration
# ==========================================================

AWS_CONN_ID = "minio_conn"
SNOWFLAKE_CONN_ID = "snowflake_conn"

MINIO_BUCKET = "banking-data"

SNOWFLAKE_DATABASE = "BANKING_DWH"
SNOWFLAKE_SCHEMA = "BRONZE"

STAGE_NAME = "MINIO_RAW_STAGE"
FILE_FORMAT = "PARQUET_SNAPPY_FORMAT"

TABLE_CONFIG = {
    "customers": "BRONZE_CUSTOMERS_RAW",
    "accounts": "BRONZE_ACCOUNTS_RAW",
    "transactions": "BRONZE_TRANSACTIONS_RAW"
}


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
    "email_on_retry": False
}


# ==========================================================
# Discover Files
# ==========================================================

def discover_files(s3_hook, minio_prefix):
    """
    Returns parquet files under raw/<table>/  sorted oldest first.
    """
    prefix = f"raw/{minio_prefix}/"
    keys = s3_hook.list_keys(bucket_name=MINIO_BUCKET, prefix=prefix) or []

    parquet_files = [key for key in sorted(keys) if key.endswith(".parquet")]
    logger.info("%s parquet files found for %s", len(parquet_files), minio_prefix)
    
    return parquet_files


# ==========================================================
# Load into Snowflake
# ==========================================================

def load_to_snowflake(snow_hook, local_path, filename, object_key, table_name):
    """
    Uploads local parquet to Snowflake, loads into Bronze, removes stage file.
    Returns: True / False
    """
    start_time = datetime.now(timezone.utc)
    rows_loaded=0
    
    try:
        put_sql = f"""
        PUT file://{local_path} @{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{STAGE_NAME} 
        AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
        """                                                                 #give space before @
        snow_hook.run(put_sql)

        logger.info("PUT successful.")

        copy_sql = f"""
        COPY INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{table_name}
        (ingestion_timestamp, kafka_topic, kafka_partition, kafka_offset, kafka_key, kafka_timestamp, payload, source_file)
        FROM
        (
            SELECT
            $1:ingestion_timestamp,
            $1:kafka_topic,
            $1:kafka_partition,
            $1:kafka_offset,
            $1:kafka_key,
            TO_TIMESTAMP_NTZ($1:kafka_timestamp),
            TRY_PARSE_JSON($1:payload::VARCHAR),
            '{object_key}'
            FROM @{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{STAGE_NAME}/{filename}
        )
        FILE_FORMAT=(FORMAT_NAME={SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{FILE_FORMAT})
        ON_ERROR='ABORT_STATEMENT';
        """
        
        end_time = datetime.now(timezone.utc)
        duration = (end_time-start_time).total_seconds()

        copy_result = snow_hook.get_records(copy_sql)
        logger.info("COPY INTO completed: %s",copy_result)
        
        rows_loaded = copy_result[0][3] if copy_result else 0

        return {
            "success": True,
            "rows_loaded": rows_loaded,
            "started_at": start_time,
            "completed_at": end_time,
            "duration": duration
        }
    except Exception as ex:
        audit_sql = f"""
            INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.INGESTION_AUDIT
            (table_name, file_name, source_path, rows_loaded, status, error_message, started_at, completed_at, duration_seconds)
            VALUES
            ('{table_name}', '{filename}', '{object_key}', 0, 'FAILED', '{str(ex).replace("'", "''")}', '{start_time}', CURRENT_TIMESTAMP(), 0);
            """
        snow_hook.run(audit_sql)
        logger.exception("Snowflake load failed for %s: %s", filename, ex)
        
        return {
            "success": False,
            "rows_loaded": rows_loaded,
            "started_at": start_time,
            "completed_at": end_time,
            "duration": duration
        }
    
    finally:
        try:
            remove_sql = f"""
            REMOVE @{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.{STAGE_NAME}/{filename};
            """
            snow_hook.run(remove_sql)

        except Exception:
            logger.warning("Unable to remove %s from Snowflake stage.", filename)
    

# ==========================================================
# Archive Successfully Processed File
# ==========================================================

def archive_file(s3_client, object_key):
    """
    Moves file from raw/ to processed/.
    Returns True if archive succeeded.
    """
    destination_key = object_key.replace("raw/", "processed/", 1)

    # Copy object
    s3_client.copy_object(
        Bucket=MINIO_BUCKET,
        CopySource={
            "Bucket": MINIO_BUCKET,
            "Key": object_key
        },
        Key=destination_key
    )

    # Verify copied object exists
    s3_client.head_object(Bucket=MINIO_BUCKET, Key=destination_key)

    # Delete original
    s3_client.delete_object(Bucket=MINIO_BUCKET, Key=object_key)

    logger.info("Archived %s", destination_key)

    return True

# ==========================================================
# Process One Table
# ==========================================================

def already_loaded(snow_hook, object_key):
    """
    Returns True if this file has already been successfully loaded.
    """

    sql = f"""
    SELECT COUNT(*)
    FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.INGESTION_AUDIT
    WHERE source_path = '{object_key}' AND status='SUCCESS';
    """

    result = snow_hook.get_first(sql)
    return result[0] > 0


def process_table(minio_prefix, snowflake_table):
    """
    Loads every parquet file for one dataset.
    """
    # FIXED: Initialize connections once per task to prevent connection exhaustion
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_client = s3_hook.get_conn()
    snow_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

    files = discover_files(s3_hook, minio_prefix)

    if not files:
        logger.info("No files found for %s", minio_prefix)
        return

    logger.info( "Processing %s files for %s", len(files), minio_prefix)

    for object_key in files:
        local_path = None
        filename = os.path.basename(object_key)
        local_path = os.path.join(tempfile.gettempdir(), filename)

        try:
            if already_loaded(snow_hook, object_key):
                logger.info("%s already loaded. Skipping.", object_key)
                continue
            # Download from MinIO
            s3_client.download_file(MINIO_BUCKET, object_key, local_path)

            # Load into Snowflake
            result = load_to_snowflake(
                snow_hook=snow_hook,
                local_path=local_path,
                filename=filename,
                object_key=object_key,
                table_name=snowflake_table
            )

            # Archive only after successful load
            if result["success"]:
                archive_file(s3_client, object_key)

                audit_sql = f"""
                    INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.INGESTION_AUDIT
                    (table_name, file_name, source_path, rows_loaded, status, error_message, started_at, completed_at, duration_seconds)
                    VALUES
                    ('{snowflake_table}', '{filename}', '{object_key}', {result["rows_loaded"]}, 'SUCCESS', NULL, '{result["started_at"]}', '{result["completed_at"]}', {result["duration"]});
                    """
                snow_hook.run(audit_sql)

                logger.info("Finished %s", filename)
            else:
                logger.warning("Skipping archive because Snowflake load failed : %s", filename)

        except Exception as ex:
            audit_sql = f"""
                INSERT INTO {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.INGESTION_AUDIT
                (table_name, file_name, source_path, rows_loaded, status, error_message, started_at, completed_at, duration_seconds)
                VALUES
                ('{snowflake_table}', '{filename}', '{object_key}', 0, 'FAILED', '{str(ex).replace("'", "''")}', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL);
                """
            snow_hook.run(audit_sql)
            logger.exception( "Failed processing %s : %s", object_key, ex)

        finally:
            if local_path and os.path.exists(local_path):
                os.remove(local_path)


# ==========================================================
# DAG
# ==========================================================

with DAG(
    dag_id="minio_to_snowflake_bronze",
    description="Load CDC Parquet files from MinIO to Snowflake Bronze",
    start_date=datetime(2025, 1, 1),
    schedule="*/2 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["banking", "bronze", "snowflake", "cdc"]
) as dag:
    
    previous_task = None

    for dataset, table in TABLE_CONFIG.items():
        task = PythonOperator(
            task_id=f"load_{dataset}",
            python_callable=process_table,
            op_kwargs={
                "minio_prefix": dataset,
                "snowflake_table": table
            }
        )

        if previous_task:
            previous_task >> task

        previous_task = task