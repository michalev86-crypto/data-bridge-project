from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import days_ago

from pymongo import MongoClient
import json
import logging

# -------------------
# CONFIG
# -------------------

MONGO_URI = 'mongodb://mongodb:27017/'
MONGO_DB_NAME = 'crane_data'
MONGO_COLLECTION = 'raw_telemetry'

SNOWFLAKE_CONN_ID = "snowflake_default"
SNOWFLAKE_DB_NAME = "CRANE_RAW_DB"
SNOWFLAKE_SCHEMA_NAME = "STAGING"
SNOWFLAKE_TABLE = "RAW_TELEMETRY_STAGING"


# -------------------
# TASK FUNCTION
# -------------------
def extract_mongo_data_and_load_to_snowflake():
    logging.info("Connecting to MongoDB...")
    # NOTE: Using the internal Docker service name 'mongodb'
    client = MongoClient(MONGO_URI)
    collection = client[MONGO_DB_NAME][MONGO_COLLECTION]

    # Fetch all documents
    mongo_data = list(collection.find())
    logging.info(f"Fetched {len(mongo_data)} documents from MongoDB")

    if not mongo_data:
        logging.info("No data found, exiting task")
        return

    # Prepare JSON records for Snowflake
    records = []
    for doc in mongo_data:
        # Convert MongoDB's special ObjectId to a standard string
        doc["_id"] = str(doc["_id"])
        # Each record must be a list of values for the target fields. 
        # Here, we insert the full JSON string into the single RAW_JSON column.
        records.append([json.dumps(doc)])

    # Connect to Snowflake
    sf_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    logging.info("Successfully connected to Snowflake.")

    # Create table if it doesn't exist (Runs SQL via hook)
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DB_NAME}.{SNOWFLAKE_SCHEMA_NAME}.{SNOWFLAKE_TABLE} (
        RAW_JSON TEXT,
        LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    """
    sf_hook.run(create_table_sql)
    logging.info(f"Ensured table {SNOWFLAKE_TABLE} exists.")

    # Insert rows in batches
    # This uses the underlying Snowflake connection to insert data.
    row_count = sf_hook.insert_rows(
        table=f"{SNOWFLAKE_DB_NAME}.{SNOWFLAKE_SCHEMA_NAME}.{SNOWFLAKE_TABLE}",
        rows=records,
        target_fields=["RAW_JSON"],
        commit_every=1000
    )
    logging.info(f"Successfully loaded {row_count} rows into Snowflake.")


# -------------------
# DAG DEFINITION
# -------------------
with DAG(
    dag_id='mongo_to_snowflake_raw_load',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['mongodb', 'snowflake', 'raw'],
) as dag:

    load_raw_data_to_snowflake = PythonOperator(
        task_id='extract_and_load_raw_data',
        python_callable=extract_mongo_data_and_load_to_snowflake,
    )
