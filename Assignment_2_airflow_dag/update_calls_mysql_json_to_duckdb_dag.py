from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.sdk import Variable
import duckdb
import pymysql  # MySQL connector
import os
import json
import pandas as pd


JSON_BASE_PATH = "/usr/local/airflow/include/telephony_mock_api/"
DUCKDB_PATH = "/usr/local/airflow/include/support_calls.duckdb"


def detect_new_calls():
    last_loaded_time = Variable.get("support_calls_watermark", "1970-01-01 00:00:00")
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    sql = "SELECT call_id, employee_id, call_time, phone, direction, status FROM calls WHERE call_time > %s"
    new_calls = mysql_hook.get_records(sql, parameters=[last_loaded_time])

    if not new_calls:
        print("No new calls found.")
        return []

    # Store in XCom as list of dicts for next task
    column_names = ['call_id', 'employee_id', 'call_time', 'phone', 'direction', 'status']
    return [dict(zip(column_names, row)) for row in new_calls]


def load_telephony_details(**context):
    new_calls = context["ti"].xcom_pull(task_ids="detect_new_calls")
    if not new_calls:
        return []

    updated_data=[]
    for call in new_calls:
        call_id = call['call_id']
        file_path = f"{JSON_BASE_PATH}{call_id}.json"

        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                json_data = json.load(f)

                call.update(json_data)
                updated_data.append(call)
        else:
            print(f"Warning: JSON for call_id {call_id} not found.")

    return updated_data


def transform_and_load_duckdb(**context):
    # get calls from previous task
    new_calls = context["ti"].xcom_pull(task_ids="load_telephony_details")
    if not new_calls:
        print("No calls to process in this run.")
        return
    calls_df = pd.DataFrame(new_calls)

    # get employees from MySQL
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    employees_df = mysql_hook.get_pandas_df("SELECT employee_id, full_name, team FROM employees")
    #join and Data quality checks
    enriched_df = calls_df.merge(employees_df, on='employee_id', how='inner')
    enriched_df = enriched_df[enriched_df['duration_sec'] >= 0]

    if enriched_df.empty:
        print("After Data Quality checks, no valid calls left to process.")
        return


    # create final empty table in DuckDB is it doesn't exist
    db = duckdb.connect(DUCKDB_PATH)
    db.execute("""
               CREATE TABLE IF NOT EXISTS support_call_enriched
               (
                   call_id INTEGER PRIMARY KEY,
                   employee_id INTEGER, 
                   full_name VARCHAR,
                   team VARCHAR,
                   call_time TIMESTAMP,
                   phone VARCHAR,
                   direction VARCHAR,
                   status VARCHAR,
                   duration_sec INTEGER,
                   short_description TEXT,
                   updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
               )
               """)

    # Register merged calls as view
    db.register('enriched_df_view', enriched_df)

    # upsert
    db.execute("""
               INSERT INTO support_call_enriched
               (call_id, employee_id, full_name, team, call_time, phone, direction, status, duration_sec,
                short_description)
               SELECT call_id,
                      employee_id,
                      full_name,
                      team,
                      call_time,
                      phone,
                      direction,
                      status,
                      duration_sec,
                      short_description
               FROM enriched_df_view ON CONFLICT (call_id) DO
               UPDATE SET
                   full_name = excluded.full_name,
                   team = excluded.team,
                   duration_sec = excluded.duration_sec,
                   short_description = excluded.short_description,
                   updated_at = now()
               """)

    # update watermark
    max_time = enriched_df['call_time'].max()
    Variable.set("support_calls_watermark", str(max_time))
    print(f" Watermark updated to: {max_time}")

    # Observability
    print(f"Observability: Successfully upserted {len(enriched_df)} rows into DuckDB.")
    db.close()

# Define Airflow DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 3, 1),
    "catchup": False,
    # Retry & alert strategy
    "retries": 2,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
    dag_id="update_calls_mysql_json_to_duckdb_dag",
    default_args=default_args,
    schedule='@hourly',
    catchup=False,
)

# Define tasks
t1 = PythonOperator(
    task_id="detect_new_calls",
    python_callable=detect_new_calls,
    dag=dag
)
t2 = PythonOperator(
    task_id="load_telephony_details",
    python_callable=load_telephony_details,
    dag=dag
)
t3 = PythonOperator(
    task_id="transform_and_load_duckdb",
    python_callable=transform_and_load_duckdb,
    dag=dag
)

# Task dependencies
t1 >> t2 >> t3