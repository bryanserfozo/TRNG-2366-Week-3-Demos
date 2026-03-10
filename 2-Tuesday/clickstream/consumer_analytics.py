# This class will be our first Kafka Consumer to consumer the messages provided inside of the producer
import json
from collections import defaultdict
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
    # Create consumer
    consumer = create_consumer()

    # Let's subscribe to our favorite topics
    consumer.subscribe(['website_clicks'])

    # Let's create a dictionary to store our information
    page_counts = defaultdict(int)

    try:
        while True:
            # First thing we are going to do is poll the Kafka server to see if there are new messages
            msgs = consumer.poll(1) # Poll the server every 1 ms to see if there are new messages

            # These messages will include information regarding the topic partition and the actual message itself
            if msgs is None:
                continue

            for tp, messages in msgs.items():
                # print(f"Received message in partition: {tp.partition}")
                # I could print out the topic partition here if desired but it's not necessary
                # Let's loop over the messages and keep track of them in our analytics
                for msg in messages:
                    event = msg.value

                    page = event['page']
                    page_counts[page] += 1

                    print("Page View stats")
                    print(dict(page_counts))

    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()