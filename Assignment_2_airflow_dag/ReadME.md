# Assignment: Hourly Support Call Enrichment Pipeline (Airflow/MySQL/JSON/DuckDB)
## 🚀 Overview
Support teams need near-real-time visibility into call quality and context.
This pipeline enriches raw support call logs with telephony metadata and an LLM-style call summary, then loads a clean analytical table for reporting, QA, and monitoring.

## 🛠 Architecture & Technologies
* Orchestration: Apache Airflow (PythonOperator, Variables, XCom)

* Data Sources: * MySQL Database (Relational data: employees, calls)

* Local JSON Files (Mock Telephony API)

* Analytical Database: DuckDB

* Data Processing: Pandas

## ⚙️ DAG Structure: update_calls_mysql_json_to_duckdb_dag
The DAG is scheduled to run @hourly and consists of three idempotent tasks:

1. detect_new_calls: Connects to MySQL using MySqlHook. It utilizes an Airflow Variable (support_calls_watermark) to implement incremental loading, fetching only calls that occurred after the last successful run.

2. load_telephony_details: Pulls the list of new calls via XCom, dynamically reads the corresponding JSON files from the local directory (include/telephony_mock_api/), and merges the mock LLM descriptions and call durations into the dataset.

3. transform_and_load_duckdb:

  * Loads the employees table from MySQL.

  * Performs an INNER JOIN in Pandas to combine call data with employee details.

  * Loads the enriched data into a DuckDB view.

  * Performs an Upsert (ON CONFLICT DO UPDATE) to guarantee idempotency.

  * Updates the Airflow support_calls_watermark safely at the end of the transaction.

## ✨ Advanced Features Implemented 
1. Data Quality Checks: The pipeline filters out anomalous data. Using an INNER JOIN drops calls assigned to non-existent employees, and a Pandas filter (duration_sec >= 0) drops corrupted telephony records with negative durations.

2. Idempotency: Using ON CONFLICT (call_id) DO UPDATE SET ... ensures that rerunning a failed DAG will never duplicate records in the final DuckDB table.

3. Retry & Alert Strategy: Configured in default_args with retries: 2 and a retry_delay of 1 minute to handle temporary network or database unavailability.

4. Observability: The pipeline logs the exact number of rows successfully upserted into DuckDB to the stdout for easy monitoring in the Airflow UI.

5. Security: No credentials are hardcoded. MySQL connection relies entirely on Airflow's secure Connections module (mysql_default).

📚 Theory Questions (Airflow)
1. What is a DAG in Apache Airflow and what problem does it solve?
A DAG (Directed Acyclic Graph) is a collection of all the tasks you want to run, organized in a way that reflects their relationships and dependencies. It ensures tasks run in the correct order, prevents infinite loops (acyclic), and solves the problem of orchestrating complex data workflows.

2. What is a task in Airflow and how is it different from a DAG?
A DAG is the blueprint or container for the workflow, while a task is a single node within that DAG. A task represents one distinct unit of work (e.g., executing a Python script, running a SQL query).

3. What is an Operator in Airflow? Give one example.
An Operator is a template for a predefined task. It determines what actually gets done. In this assignment, the PythonOperator was used to execute custom Python functions.

4. What is the difference between Operators and Hooks in Airflow?
Operators determine the action to be executed, whereas Hooks are interfaces used to interact with external systems (like databases or APIs). For example, a task uses an Operator to run, but that Operator might use a MySqlHook to securely manage the database connection underneath.

5. What is XCom and when should it be used? Give an example.
XCom (Cross-Communication) is a mechanism that allows tasks to talk to each other by exchanging small amounts of metadata. It should be used for passing states or small lists. In this DAG, XCom is used to pass the list of newly extracted call_id dictionaries from detect_new_calls to the load_telephony_details task.

6. What is an Airflow Connection and why should credentials not be hardcoded in DAGs?
An Airflow Connection is a secure storage mechanism within Airflow's metadata database for storing hostnames, logins, and passwords. Hardcoding credentials in DAG code is a major security risk, as the code is often shared in version control (like GitHub) where unauthorized users could steal them.

7. What are Airflow Variables and when would you use them instead of XCom?
Variables are a generic way to store and retrieve arbitrary settings or state as a simple key-value store. Unlike XComs, which are tied to a specific execution date and task, Variables are global. In this pipeline, a Variable is used to store the support_calls_watermark so it persists across different hourly runs.

8. What is a cron expression and how is it used to schedule an Airflow DAG?
A cron expression is a string consisting of five fields (minute, hour, day of month, month, day of week) that represents a time schedule. To run a DAG every hour, the expression is 0 * * * * (which is equivalent to Airflow's @hourly macro).

9. What does the catchup parameter do in Airflow? Why is catchup=False often used?
The catchup parameter tells Airflow whether it should run all missed DAG executions between the start_date and the current time. Setting catchup=False is standard for hourly production pipelines because it prevents the system from being overwhelmed by executing hundreds of historical runs simultaneously if the server was offline for a few days.
