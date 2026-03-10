# This class will be our producer class, it will simulate clicks on our ecommerce application and allow us to track metrics as they come
import json
import random
import time
from kafka import KafkaProducer

def create_producer(
        bootstrap_servers = 'localhost:9092',
        linger_ms = 50,
        batch_size = 16384,
        compression_type = 'lz4'
        ):
    # This will create a KafkaProducer object we can use to send a message
    return KafkaProducer(
        # This is the Kafka broker(s) we're connecting to
        bootstrap_servers = bootstrap_servers,

        # Below are additional setting that can be used to improve the perfomance of our producer
        # Linger is a ms length that the producer will batch messages for before sending them
        linger_ms = linger_ms,

        # This is the maximum batch size (bytes), in this case 16384 is 16KB
        batch_size = batch_size,

        # Compression type allows us to compress the messages and send them more efficiently
        compression_type = compression_type,

        # The following are used to encode our key and values to bytes
        key_serializer = lambda k: k.encode("utf-8") if k else None,
        value_serializer = lambda v: json.dumps(v).encode("utf-8")
    )

def main():
    producer = create_producer()

    # Let's list our pages and devices to generate a simulated click
    pages = ["/", "/products", "/cart", "/about"]
    devices = ["mobile", "desktop", "tablet"]


    # Recall an event can be basically any action, in this case it's a click
    # Let's put this in a loop so we can do it as many times as we want
    try:
        count = 0
        start = time.time()
        while True:
            
            # Creating an event
            event = {
                "user_id": random.randint(1,999),
                "page": random.choice(pages),
                "device": random.choice(devices),
                "timestamp": int(time.time())
            }

            # Send the event using our producer
            producer.send(
                topic="website_clicks",
                # key=event["page"],
                value=event
            )

            # Commenting out the sleep so this produces messages as fast as it can
            # time.sleep(1) # Waits 5 seconds before initiating a new message

            count += 1

            if count % 10000 == 0:
                now = time.time()
                elapsed = now - start

                print(f"Sent {count} messages ({count/elapsed:.0f} msgs/sec)")


    except KeyboardInterrupt:
        pass
    finally:
        producer.flush() 


if __name__ == "__main__":
    main()