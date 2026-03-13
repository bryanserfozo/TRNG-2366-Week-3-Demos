from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook

from kafka import KafkaConsumer
from datetime import datetime
import json
import os

def consume_events():
    # To consume we need to create a Kafka Consumer just like we did before
    conn = BaseHook.get_connection("kafka_game_events")

    extras = conn.extra_dejson

    consumer = KafkaConsumer(
        "game_events", # Topic
        bootstrap_servers = extras.get("bootstrap.servers"),
        auto_offset_reset = extras.get("auto.offset.reset"),
        group_id = "kafka_consumer_demo",
        enable_auto_commit = True,
        value_deserializer = lambda v: json.loads(v.decode("utf-8"))
    )

    # Now that we've defined our consumer we need to consume the generated events that are in the queue
    os.makedirs("/opt/spark-data/landing", exist_ok=True)

    filename = "/opt/spark-data/landing/sample.json"
    events = 0
    with open(filename, "a") as f:
        # msgs = consumer.poll() # Needed some time
        msgs = consumer.poll(10000, max_records= 10000) # Needed some time
        for tp, messages in msgs.items():
            # GOAL add all of the pieces of data to a json file to be read by our ETL Job
            for message in messages:
                f.write(json.dumps(message.value) + "\n")
                events += 1

    print(f"Consumed {events} events")
 


def validate_outputs():
    print("Imagine validate functionality here")

with DAG(
    dag_id="kafka_consumer_and_etl",
    start_date = datetime(2026,3,13),
    schedule = "*/5 * * * *", # Runs every 5 minutes
    catchup = False,
    tags = ["demo", "development", "kafka", "consumer"]
) as dag:
    
    start = EmptyOperator(task_id = "start")
    end = EmptyOperator(task_id="end")

    consume = PythonOperator(
        task_id="consume_events",
        python_callable = consume_events
    )

    run_spark = BashOperator(
        task_id = "run_spark_job",
        bash_command = """
        spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --name "KafkaSparkETL" \
            /opt/spark-jobs/sample_etl_job.py \
            /opt/spark-data/landing \
            /opt/spark-data/gold \
            {{ ds }}
    """
    # {{ ds }} is the way to use the DAG's exectution datestring
    )

    validate = PythonOperator(
        task_id = "validate_outputs",
        python_callable=validate_outputs
    )


    start >> consume >> run_spark>> validate >> end