from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import random
import time

def extract_us(**context):
    print("Extracting data from US")

    # Let's add some variance to how long this will take
    time.sleep(random.uniform(1,5))
    
    # Simulate getting some records
    record_count = random.randint(1000,5000)
    print(f"    Extracted {record_count} records from the US")
    # Pushing data to XCom
    return {"country": "US", "records": record_count}

def extract_mx(**context):
    print("Extracting data from Mexico")

    # Let's add some variance to how long this will take
    time.sleep(random.uniform(1,5))
    
    # Simulate getting some records
    record_count = random.randint(1000,5000)
    print(f"    Extracted {record_count} records from Mexico")
    return {"country": "MX", "records": record_count}


def extract_ca(**context):
    print("Extracting data from Canada")

    # Let's add some variance to how long this will take
    time.sleep(random.uniform(1,5))
    
    # Simulate getting some records
    record_count = random.randint(1000,5000)
    print(f"    Extracted {record_count} records from Canada")
    return {"country": "CA", "records": record_count}

def validate_all_extracts(**context):
    # Grab the data that was returned by the other tasks from XCom

    ti = context['ti']
    # What is ti? ti stands for TaskInstance and allows us to pull data from specific tasks
    # dag -> DAG object
    # task -> task definition
    # ds -> execution date string
    # run_id -> DAG run identifier

    # Let's get all of the data from the individual tasks above
    us_results = ti.xcom_pull(task_ids="extract_us")
    mx_results = ti.xcom_pull(task_ids="extract_mx")
    ca_results = ti.xcom_pull(task_ids="extract_ca")

    print("Validating all records")
    print(f"    US: {us_results['records']}")
    print(f"    MX: {mx_results['records']}")
    print(f"    CA: {ca_results['records']}")

    total_records = (
        us_results['records'] +
        ca_results['records'] +
        mx_results['records']
        )
    print(f"Total Records: {total_records}")
    return {"total_records": total_records, "status": "validated"}



with DAG(
    dag_id="thursday_demo_1",
    description="Testing dependency based DAGs as well as triggers and branching",
    start_date= datetime(2026,3,12),
    schedule=None, #Manual Trigger only
    catchup=False,
    tags=['devlopment', 'demo', 'branching', 'dependencies'],
    default_args = {
        "owner": "airflow_demo",
        "retries": 1
    }
) as dag:
    
    # Recall that we can use our Operators to build out our tasks

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    extract_us_task = PythonOperator(
        task_id="extract_us",
        python_callable=extract_us
    )

    extract_mx_task = PythonOperator(
        task_id="extract_mx",
        python_callable=extract_mx
    )
    extract_ca_task = PythonOperator(
        task_id="extract_ca",
        python_callable=extract_ca
    )

    validate_all_extracts_task = PythonOperator(
        task_id="validate",
        python_callable=validate_all_extracts
    )

    # Define the flow of our DAG here
    # start >> extract_us_task >> extract_mx_task >> extract_ca_task >> validate_all_extracts_task >> end

    # Let's update the flow of our DAG to run the extraction tasks concurrently
    # Leverage Parallelism!
    # In this case all extract tasks run concurrently and must all finish and pass before the validate step happen
    start >> [extract_us_task, extract_mx_task, extract_ca_task] >> validate_all_extracts_task >> end


dag.doc_md= """
## Dependency Demo DAG

This demo is used to show off the basics of dependency driven DAGs
"""