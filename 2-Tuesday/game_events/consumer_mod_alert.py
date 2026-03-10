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
    consumer = create_consumer(group_id="mod_alert_group")
    consumer.subscribe(['game_events'])

    loot_received = defaultdict(int)
    loot_timestamps = defaultdict(int)

    while True:
        msgs = consumer.poll()

        if msgs is None:
            continue

        for tp, messages in msgs.items():
            for msg in messages:
                event = msg.value
                if event["event"] == "loot":
                    loot_received[event["player"]] += 1

                    # FIXED THIS MOD ALERT IN THE FILE consumer_mod_alert_fixed.py

                    # Let's check the difference between this loot timestamp and the stored loop timestamp
                    if event["timestamp"] - loot_timestamps[event["player"]]  > 1 * 10:
                        # This means the last time the player received loot was more than a minute ago
                        # Update the timestamp to the new timestamp and set the loot earned to 1
                        loot_received[event["player"]] -= 1
                        loot_timestamps[event["player"]] = event["timestamp"]
                    else:
                        # If one person earns more than 10 pieces of loot in a minute we should flag them as potentially cheating
                        if loot_received[event["player"]] >= 2:
                            print(f'Potentially suspicious player detected: {event["player"]} has earned {loot_received[event["player"]]} in the last minute')



if __name__ == "__main__":
    main()