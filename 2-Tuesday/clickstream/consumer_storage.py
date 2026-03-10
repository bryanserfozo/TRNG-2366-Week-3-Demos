# The other consumer is working to provide real time data analytics regarding the views to each page
# This consumer will work on a different piece of our pipeline, storing the entries long term
import json
from kafka import KafkaConsumer

def create_consumer(
        bootstrap_servers = "localhost:9092",
        group_id = "analytics_group"
):
    # Returns a Kafka Consumer
    return KafkaConsumer(
        # Bootstraps servers for my kafka brokers
        bootstrap_servers = bootstrap_servers,
        
        # Controls the consumer group. As a reminder, memebers of the same consumer group can be assigned to different
        # partitions to achieve parallelism
        group_id = group_id,

        # Additional details (Review on your own)
        auto_offset_reset = "latest",
        enable_auto_commit = True,
        value_deserializer = lambda v: json.loads(v.decode("utf-8")),
        key_deserializer= lambda k: k.decode("utf-8") if k else None
    )

def main():
    # Create our consumer and subcribe to a topic
    consumer = create_consumer(group_id="storage_group")
    consumer.subscribe(["website_clicks"])

    with open("clickstream_events.json", "a") as f:
        while True:
            # Poll the server to get the new messages
            # Store the message in the file
            msgs = consumer.poll()

            if msgs is None:
                continue

            for tp,messages in msgs.items():
                for msg in messages:
                    event = msg.value

                    f.write(json.dumps(event) + "\n")
                    print("Stored Event")


if __name__ == "__main__":
    main()