from kafka.admin import KafkaAdminClient, NewTopic

def main():
    # Goal  -> Create a topic that can handle our website clicks
    # Config -> I can control the name, number of partitions, replication factor, retention period, delete policy

    # Recall the kafka admin client is used to perform admin operations
    admin = KafkaAdminClient(
        bootstrap_servers = 'localhost:9092',
        client_id = 'clickstream-demo'
    )

    # Create a new topic
    topic = NewTopic(
        name="website_clicks", # Name of the topic that's being connected to
        num_partitions=3, # Controls the number of partitions (used for parallel processing), 0 Indexed
        replication_factor=1 # Replication factor of 1 means no replicas, you'd use this for fault tolerance with multiple brokers
    )

    # Use the admin client to create the topic itself

    admin.create_topics([topic])

if __name__ == "__main__":
    main()