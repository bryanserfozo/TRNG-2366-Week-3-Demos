from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.exceptions import AirflowException


def always_passes():
    import time
    print("This task should always pass but we'll make it wait a few seconds")
    time.sleep(15)

def sometimes_fails():
    import random
    print("This task has a 30 percent chance to fail")

    if random.random() < .3:
        raise AirflowException
    


with DAG(
    dag_id = "sample_dag",
    description = "A Sample dag for us to test various operators and see how things work",
    start_date = datetime(2026,3,1),
    schedule = "@daily", # "* * * * *", # This should run automatically every minute but we can use presets like @daily for daily runs
    catchup = True,
    tags = ["sample", "development"]
) as dag:
    
    # Empty operator is one used mainly for organization and it's going to do no operator
    start = EmptyOperator(task_id = "start")

    pass_task = PythonOperator(
        task_id = "always_passes",
        python_callable = always_passes
    )

    fail_task = PythonOperator(
        task_id = "sometimes_fails",
        python_callable = sometimes_fails
    )


    # email_task = EmailOperator(
    #     task_id = "notify_failure",
    #     to = "bryan.serfozo@revature.com",
    #     subject="Airflow Demo Failed",
    #     html_content = """
    #     <h3>Airflow Demo Failed</h3>
    #     <p>One of the tasks in your sample dag has failed</p>
    #     """,
    #     trigger_rule = "one_failed"
    # )

    end = BashOperator(
        task_id = "end",
        bash_command ="echo 'Pipeline has been completed'"
    )

    # Provide the dependencies in order
    start >> pass_task >> fail_task >> end