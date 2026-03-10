import json
from collections import defaultdict, deque
from kafka import KafkaConsumer

TIME_WINDOW_SECONDS = 60
LOOT_THRESHOLD = 10

def create_consumer(
        bootstrap_servers = "localhost:9092",
        group_id = "analytics_group"
):
    return KafkaConsumer(
        bootstrap_servers = bootstrap_servers,
        group_id = group_id,
        auto_offset_reset = "latest",
        enable_auto_commit = True,
        value_deserializer = lambda v: json.loads(v.decode("utf-8")),
        key_deserializer= lambda k: k.decode("utf-8") if k else None
    )


def main():
    consumer = create_consumer(group_id="mod_alert_group")
    consumer.subscribe(['game_events'])

    # Use a double ended queue (deque), to keep track of the timestamps per player
    # Can check the length and remove any timestamps greated than the threshold
    loot_timestamps = defaultdict(deque)

    while True:
        msgs = consumer.poll()

        if msgs is None:
            continue

        for tp, messages in msgs.items():
            for msg in messages:
                event = msg.value
                if event["event"] != "loot":
                    continue
                player = event["player"]
                event_timestamp = event["timestamp"]

                # Get all stored timestamps for a player
                player_loot_timestamps = loot_timestamps[player]

                # Add the new timestamp
                player_loot_timestamps.append(event_timestamp)

                # Remove timestamps greater than the time window from the front of the queue
                while player_loot_timestamps and event_timestamp - player_loot_timestamps[0] > TIME_WINDOW_SECONDS:
                    player_loot_timestamps.popleft()

                # Check the length of the timestamp queue to see how many have been earned in the sliding window
                if len(player_loot_timestamps) >= LOOT_THRESHOLD:
                    print(f'Potentially suspicious player detected: {player} has earned {len(player_loot_timestamps)} in the last {TIME_WINDOW_SECONDS} seconds')


if __name__ == "__main__":
    main()