from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from kafka import KafkaProducer
import json
from datetime import datetime
import random
import time

def produce_events():
    # To set up our Kafka connection we'll need to use a hook
    # There is probably a Kafka Connection hook that can be used by import the external kafka provider
    # But we'll do this with a BaseHook to show some other details
    conn = BaseHook.get_connection("kafka_game_events")
    # I can grab details directly from the connection as if it were just a dictionary of values
    
    bootstrap_servers = conn.extra_dejson.get("bootstrap.servers")

    # Let's set up a Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers = bootstrap_servers,
        value_serializer = lambda v : json.dumps(v).encode("utf-8")
    )

    num_of_events = random.randint(50,500)

    for i in range(num_of_events):
        event = {
            "player_id": random.randint(1, 1000),
            "event_type" : random.choice(["kill", "death", "loot"]),
            "item": random.choice(["gold", "sword", "armor"]),
            "timestamp": time.time()
        }

        # Producer needs to send the event
        producer.send("game_events", event)

    # In the event any messages haven't been completely sent this will block and wait till that finished
    producer.flush()

    print(f"Produced {num_of_events} events!")




with DAG(
    dag_id = "kafka_event_producer",
    start_date= datetime(2026,3,13),
    schedule = "* * * * *",
    catchup = False,
    tags = ["development", "kafka", "demo", "producer"]
) as dag:
    
    start = EmptyOperator(task_id = "start")
    end = EmptyOperator(task_id = "end")

    produce = PythonOperator(
        task_id="produce",
        python_callable=produce_events
    )

    start >> produce >> end