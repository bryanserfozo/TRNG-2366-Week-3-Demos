from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import random
import time

# Countries
# WANT -> Pull this from somee database
# Needs ->  A connection / connection details to the DB
# Hook -> Airflow tool to allow you to use a connection inside of your python function
# COUNTRIES = ["US", "MX", "CA"]

def get_country_list(**context):
    # We've now set up a connection in the airflow UI with the name country-postgres
    # That should allow us to connect directly to the DB and execute queries against it 
    # Now we'll use a Hook which is a tool to grab the connection and use it
    print("Fetching data from our postgres db")

    hook = PostgresHook(postgres_conn_id="country_postgres")

    records = hook.get_records("SELECT * FROM countries")

    print(records)
    
    # Let's pool the records into xcom so they can be passed down and go from there
    country_codes = []
    for record in records:
        country_codes.append(record[2])

    return country_codes



def extract_country(country:str, **context):
    print(f"Extracting data from {country}")

    time.sleep(random.uniform(1,5))
    
    record_count = random.randint(1000,5000)
    print(f"    Extracted {record_count} records from {country}")
    # Pushing data to XCom
    return {"country": country, "records": record_count}

def validate_all_extracts(**context):

    ti = context['ti']

    print("Validating all records")
    total_records = 0

    country_records = ti.xcom_pull(task_ids="extract_country")

    for record in country_records:
        print(f"    {record['country']}: {record['records']} records")
        total_records += record['records']

    print(f"Total Records: {total_records}")
    return {"total_records": total_records, "status": "validated"}


def choose_load_strategy(**context):
    ti = context["ti"]
    total_records = ti.xcom_pull(task_ids="validate")["total_records"]

    # Now that we have our total records we need to determine which task to run
    if (total_records > 10000):
        return "load_partitioned"
    else:
        return "load_simple"
    
def load_simple():
    print("We opted to do a simple load for a small amount of records")

def load_partitioned():
    print("We decided to do a partitioned load because of the large amount of records (>10k)")
    



with DAG(
    dag_id="friday_demo",
    description="Testing connections and hooks",
    start_date= datetime(2026,3,12),
    schedule=None, #Manual Trigger only
    catchup=False,
    tags=['devlopment', 'demo', 'branching', 'dependencies'],
    default_args = {
        "owner": "airflow_demo",
        "retries": 1
    }
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(
        task_id="end",
        trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        )
    
    get_country_list_task = PythonOperator(
        task_id="get_country_list",
        python_callable=get_country_list
    )

    # extract_tasks = []
    
    # for country in COUNTRIES:
    #     extract_task = PythonOperator(
    #         task_id = f"extract_{country.lower()}",
    #         python_callable = extract_country,
    #         op_kwargs={"country": country}
    #     )
    #     extract_tasks.append(extract_task)

    # Problem: The list of countries was originally defined at Parse time
    # Now it is defined at Runtime, so how do we get the proper formation of our DAG
    # Dynamic Task Mapping
    # This will allow us to up our DAG at runtime

    # Let's define some dynamic task mapping to create our tasks at runtime
    extract_tasks = PythonOperator.partial(
        task_id = "extract_country",
        python_callable= extract_country
    ).expand(op_kwargs = get_country_list_task.output.map(lambda c: {"country": c}))

    validate_all_extracts_task = PythonOperator(
        task_id="validate",
        python_callable=validate_all_extracts
    )

    # We have now extracted the data, let's go about loading it. 
    # We'll use a branching strategy to determine which loading technique to use
    # Load simple -> Less than 10k records
    # Load partitioned -> More than 10k records

    # We need a new operator, the BranchPythonOperator
    # This operator is for a python function that returns a task_id and based off that we'll need to run a branched task
    choose_load_strategy_task = BranchPythonOperator(
        task_id = "choose_load_strategy",
        python_callable=choose_load_strategy
    )

    # We'll add in our branching options
    load_simple_task = PythonOperator(
        task_id= "load_simple",
        python_callable = load_simple
    )

    load_partitioned_task = PythonOperator(
        task_id= "load_partitioned",
        python_callable = load_partitioned
    )

    # Rejoin the branches
    # This step is necessary since we are branching and one of the paths will always get skipped
    # By default Airflow will only run a task if all upstream tasks are successful
    # We'll add a trigger rule to guarantee that at least one success has happened and no failures have occurred (skips are ok)

    join = EmptyOperator(
        task_id="join",
        trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )


    

    # We can add dependency flows on multiple lines for ease of view
    start >> get_country_list_task >> extract_tasks >> validate_all_extracts_task 

    validate_all_extracts_task >> choose_load_strategy_task >> [load_partitioned_task, load_simple_task] >> join >> end


dag.doc_md= """
## Dependency Demo DAG

This demo is used to show off the basics of dependency driven DAGs
"""