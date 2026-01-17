import logging
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.mongo.hooks.mongo import MongoHook

# -------------------
# Configuration
# -------------------
SNOWFLAKE_CONN_ID = 'snowflake_default'
MONGO_CONN_ID = 'mongo_default'
MONGO_COLLECTION_NAME = 'site_sensors' # Assuming this is the collection used by kafka-consumer
MONGO_DATABASE_NAME = 'sensors_db' # Adjust this to your MongoDB database name

SNOWFLAKE_DB_NAME = 'RAW_DATA_DB'
SNOWFLAKE_SCHEMA_NAME = 'RAW'
SNOWFLAKE_TABLE = 'SITE_SENSORS_RAW'
# Field in MongoDB documents used for filtering new records
MONGO_TIMESTAMP_FIELD = 'timestamp' 

# -------------------
# Python Callables (Task Functions)
# -------------------

def get_last_load_timestamp(**kwargs) -> str:
    """
    Task 1: Retrieves the maximum load timestamp (LOAD_TS) from Snowflake.
    This value is used to filter new records from MongoDB.
    """
    sf_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    logging.info("Connected to Snowflake to determine last load point.")

    # Ensure the target table exists before querying for MAX(LOAD_TS)
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_DB_NAME}.{SNOWFLAKE_SCHEMA_NAME}.{SNOWFLAKE_TABLE} (
        RAW_JSON TEXT,
        LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    );
    """
    sf_hook.run(create_table_sql)
    logging.info(f"Ensured table {SNOWFLAKE_TABLE} exists.")

    get_max_ts_sql = f"""
    SELECT MAX(LOAD_TS) FROM {SNOWFLAKE_DB_NAME}.{SNOWFLAKE_SCHEMA_NAME}.{SNOWFLAKE_TABLE};
    """
    
    max_ts = sf_hook.get_first(get_max_ts_sql)
    
    if max_ts and max_ts[0]:
        # Convert datetime object to ISO format string for XCom/MongoDB comparison
        last_ts_str = max_ts[0].isoformat()
        logging.info(f"Last loaded timestamp found in Snowflake: {last_ts_str}")
    else:
        # If the table is empty, start from a fixed historical point
        last_ts_str = datetime.min.isoformat() 
        logging.info("Table is empty. Setting last load time to the minimum possible.")

    # Push the timestamp to XCom for the next task
    kwargs['ti'].xcom_push(key='last_load_ts', value=last_ts_str)
    return last_ts_str

def extract_and_load_incremental_data(**kwargs):
    """
    Task 2: Pulls new records from MongoDB (using the last load time) and loads them to Snowflake.
    """
    ti = kwargs['ti']
    # Pull the last load timestamp from the previous task's XCom
    last_load_ts_str = ti.xcom_pull(task_ids='get_last_load_ts', key='last_load_ts')
    
    # Convert the string timestamp back to a datetime object for comparison
    # Add a small delta to avoid reprocessing the exact last record
    last_load_dt = datetime.fromisoformat(last_load_ts_str) + timedelta(milliseconds=1)
    logging.info(f"Starting incremental load. Filtering records newer than: {last_load_dt.isoformat()}")

    # --- MongoDB Extraction ---
    mongo_hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
    
    # Filter: retrieve documents where the MONGO_TIMESTAMP_FIELD is greater than the last loaded time
    # Note: This assumes the MongoDB 'timestamp' field is stored as an ISO datetime string or BSON Date.
    query_filter = {MONGO_TIMESTAMP_FIELD: {"$gt": last_load_dt.isoformat()}} 
    
    records_cursor = mongo_hook.find(
        mongo_collection=MONGO_COLLECTION_NAME,
        query=query_filter,
        mongo_db=MONGO_DATABASE_NAME
    )
    
    # Convert MongoDB documents to JSON strings (records) for Snowflake RAW_JSON column
    records = []
    for doc in records_cursor:
        # MongoDB BSON objects need conversion (e.g., ObjectId)
        doc['_id'] = str(doc['_id'])
        records.append(json.dumps(doc, default=str))

    if not records:
        logging.info("No new records found in MongoDB. Task finished successfully.")
        return

    logging.info(f"Found {len(records)} new records to load.")

    # --- Snowflake Loading ---
    sf_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    
    # Prepare rows for insertion: list of tuples, where each tuple contains one element (the JSON string)
    rows_to_insert = [(record,) for record in records]

    # Insert rows in batches
    row_count = sf_hook.insert_rows(
        table=f"{SNOWFLAKE_DB_NAME}.{SNOWFLAKE_SCHEMA_NAME}.{SNOWFLAKE_TABLE}",
        rows=rows_to_insert,
        target_fields=["RAW_JSON"], # LOAD_TS will use the default value (CURRENT_TIMESTAMP)
        commit_every=1000
    )
    logging.info(f"Successfully loaded {row_count} new incremental rows into Snowflake.")


# -------------------
# DAG DEFINITION
# -------------------
with DAG(
    dag_id='mongo_to_snowflake_incremental_load',
    start_date=days_ago(1),
    schedule_interval=timedelta(hours=1), # Run every hour
    catchup=False,
    tags=['mongodb', 'snowflake', 'incremental', 'data-bridge'],
) as dag:

    # 1. Task: Find the maximum timestamp in the target table
    get_last_load_ts = PythonOperator(
        task_id='get_last_load_ts',
        python_callable=get_last_load_timestamp,
    )

    # 2. Task: Extract new data from Mongo and load to Snowflake
    load_incremental_data = PythonOperator(
        task_id='load_incremental_data',
        python_callable=extract_and_load_incremental_data,
    )

    # Set dependency: Find timestamp, then load data
    get_last_load_ts >> load_incremental_data
