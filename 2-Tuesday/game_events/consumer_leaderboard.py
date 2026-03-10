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
    consumer = create_consumer(group_id="leaderboard_group")
    consumer.subscribe(['game_events'])

    # Creating a dictionary to store kill information
    kills = defaultdict(int)

    while True:
        msgs = consumer.poll()

        if msgs is None:
            continue

        # Loop over all of the messages, add the kill to the specific person and then show the leaderboard
        for tp, messages in msgs.items():
            for msg in messages:
                event = msg.value
                if event["event"] == "kill":
                    kills[event["player"]] += 1
                    
                    print("Leaderboard:")
                    # Shows the top 5 members of the leaderboard
                    # Look into syntax if necessary
                    print(dict(sorted(kills.items(), key=lambda x: -x[1])[:5]))



if __name__ == "__main__":
    main()