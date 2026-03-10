import json
import random
import time
from kafka import KafkaProducer

def confirm_sent():
    print("Kafka has received our message")

def error_on_message_sent():
    print("Kafka threw an error when we tried to send the message")

def main():
    # Create our producer
    producer = KafkaProducer(
        bootstrap_servers = "localhost:9092",
        key_serializer = lambda k: k.encode("utf-8") if k else None,
        value_serializer = lambda v : json.dumps(v).encode("utf-8"),
        
    )
    """ acks is the variable determining the number of acknowledgements we receive before continuing to the next message 
            acks = "1" => Confirming the leader partition has received the value
            acks = "all" => Confirms all partitions (leader and all followers) receive the value
            acks = "0" is fire and forget, no need for confirmation
    """

    players = [f"player-{i}" for i in range (1,10)]
    events = ["kill", "loot"] # Removed death to get more loot

    while True:
        event = {
            "player": random.choice(players),
            "event": random.choice(events),
            "timestamp": int(time.time())
        }

        print(dict(event))

        producer.send(
            topic="game_events",
            value=event
        ) #.add_callback(confirm_sent).add_errback(error_on_message_sent)

        time.sleep(.5)

if __name__ == "__main__":
    main()