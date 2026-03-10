from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer
import sys

def explore_cluster(bootstrap_servers: str = "localhost:9092", max_retries : int = 5):
    try:
        admin_client = KafkaAdminClient(
                bootstrap_servers = bootstrap_servers,
                client_id = 'setup-verification'
        )
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            client_id="metadata-viewer"
        )

        # Get broker information
        brokers = consumer._client.cluster.brokers()
        print(f"\nBrokers ({len(brokers)}):")
        for broker in brokers:
            print(f"  - Broker {broker.nodeId}: {broker.host}:{broker.port}")
        
        # Get controller information
        controller = consumer._client.cluster.controller
        if controller:
            print(f"\nController: Broker {controller.nodeId}")

        topics = admin_client.list_topics()
        user_topics = [t for t in topics if not t.startswith('__')]
        internal_topics = [t for t in topics if t.startswith('__')]

        if user_topics:
            print("\n   User topic details:")
            topic_details = admin_client.describe_topics(user_topics)
            
            for topic_info in topic_details:
                name = topic_info["topic"]
                partitions = topic_info["partitions"]
                print(f"\n   Topic: {name}")
                print(f"   Partitions: {len(partitions)}")
                
                for p in partitions:
                    leader = p["leader"]
                    replicas = p["replicas"]
                    isr = p["isr"]
                    status = "HEALTHY" if len(isr) == len(replicas) else "DEGRADED"
                    print(f"     Partition {p['partition']}: "
                          f"Leader={leader}, Replicas={replicas}, ISR={isr} [{status}]")
        
        # Section 3: Configuration Insights
        print("\n3. ARCHITECTURE INSIGHTS")
        print("-" * 40)
        print("   Key points from this cluster:")
        print(f"   - Single broker setup (development mode)")
        print(f"   - Replication factor limited by broker count")
        print(f"   - Controller handles leader election")
        
        # Cleanup
        consumer.close()
        admin_client.close()
        
        return True
        
    except:
        pass

def create_demo_topic(bootstrap_servers: str = "localhost:9092"):
    # Here let's try to create a sample topic
    # Recall that a topic is a channel that events get pushed to, they're a grouping of related events (invoice events or shipping events)

    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers = bootstrap_servers,
            client_id = 'topic-creator'
        )

        # Define a new topic
        # We'll need a topic name, number of partitions and the replication factor
        topic = NewTopic(
            name="architecture-demo",
            num_partitions = 3,
            replication_factor = 1
        )

        # Use the admin client to create the topic on the broker
        admin_client.create_topics(new_topics = [topic], validate_only = False)
        print('Created new topic with 3 partitions')

        admin_client.close()
        return True
    except Exception as e:
        if "TopicAlreadyExistsError" in str(type(e).__name__):
            print(f"   Topic 'architecture-demo' already exists")
            return True
        print(f"[ERROR] Could not create topic: {e}")
        return False

def main():
    if not explore_cluster():
        print("Validate Kafka cluster is running")
        sys.exit(1)
    
    # After exploring the cluster, let's create the topic
    create_demo_topic()

    # Explore the cluster again to view the updates
    explore_cluster()

if __name__ == "__main__":
    main()