from kafka.admin import KafkaAdminClient, NewTopic

"""
We'll be creating a topic to monitor game events in a multiplayer game.
A game event could be pretty much anything, but we'll define it as a kill, a death, or obtaining valuable loot

We'll build two simple consumers. One will parse the game_events topic and get this people with the most kills, showing a PVP leaderboard
Our second consumer will be monitoring for suspicious activity, they'll be seeing any players that have picked up too much loot
in the last couple of minutes
"""

def main():
    admin = KafkaAdminClient(
        bootstrap_servers = "localhost:9092"
    )

    topic = NewTopic(
        name="game_events",
        num_partitions=1,
        replication_factor=1
    )

    admin.create_topics([topic])

if __name__ == "__main__":
    main()