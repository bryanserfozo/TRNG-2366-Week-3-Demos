from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import random
import time

COUNTRIES = ["US", "MX", "CA", "UK", "FR", "NZ", "AU"]

def extract_country(country:str, **context):
    print(f"Extracting data from {country}")

    # Let's add some variance to how long this will take
    time.sleep(random.uniform(1,5))
    
    # Simulate getting some records
    record_count = random.randint(1000,5000)
    print(f"    Extracted {record_count} records from {country}")
    # Pushing data to XCom
    return {"country": country, "records": record_count}

def validate_all_extracts(**context):
    # Grab the data that was returned by the other tasks from XCom

    ti = context['ti']
    # What is ti? ti stands for TaskInstance and allows us to pull data from specific tasks
    # dag -> DAG object
    # task -> task definition
    # ds -> execution date string
    # run_id -> DAG run identifier

    print("Validating all records")
    total_records = 0
    for country in COUNTRIES:
        # Fetch the data returned from the task
        country_records = ti.xcom_pull(task_ids=f"extract_{country.lower()}")
        print(f"    {country}: {country_records['records']} records")
        total_records += country_records['records']

    print(f"Total Records: {total_records}")
    return {"total_records": total_records, "status": "validated"}



with DAG(
    dag_id="thursday_demo_2",
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

    # Let's shorten these down and make this a little more programattic
    # extract_us_task = PythonOperator(
    #     task_id="extract_us",
    #     python_callable=extract_country,
    #     op_kwargs={"country":"US"}
    # )

    # extract_mx_task = PythonOperator(
    #     task_id="extract_mx",
    #     python_callable=extract_country,
    #     op_kwargs={"country":"MX"}
    # )
    # extract_ca_task = PythonOperator(
    #     task_id="extract_ca",
    #     python_callable=extract_country,
    #     op_kwargs={"country":"CA"}
    # )

    extract_tasks = []
    
    for country in COUNTRIES:
        extract_task = PythonOperator(
            task_id = f"extract_{country.lower()}",
            python_callable = extract_country,
            op_kwargs={"country": country}
        )
        extract_tasks.append(extract_task)

    validate_all_extracts_task = PythonOperator(
        task_id="validate",
        python_callable=validate_all_extracts
    )

    # Define the flow of our DAG here
    # start >> extract_us_task >> extract_mx_task >> extract_ca_task >> validate_all_extracts_task >> end

    # Let's update the flow of our DAG to run the extraction tasks concurrently
    # Leverage Parallelism!
    # In this case all extract tasks run concurrently and must all finish and pass before the validate step happen
    start >> extract_tasks >> validate_all_extracts_task >> end


dag.doc_md= """
## Dependency Demo DAG

This demo is used to show off the basics of dependency driven DAGs
"""