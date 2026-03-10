from kafka.admin import KafkaAdminClient, NewTopic
from kafka import KafkaConsumer
import sys


def explore_cluster(bootstrap_servers: str = "localhost:9092"):
    
    print("=" * 60)
    print("KAFKA ARCHITECTURE EXPLORATION")
    print("=" * 60)
    
    try:
        # Connect to the cluster
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="architecture-explorer"
        )
        
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            client_id="metadata-reader"
        )
        
        # Section 1: Broker Information
        print("\n1. BROKER INFORMATION")
        print("-" * 40)
        
        brokers = consumer._client.cluster.brokers()
        print(f"   Total brokers: {len(brokers)}")
        for broker in brokers:
            print(f"   - Broker {broker.nodeId}: {broker.host}:{broker.port}")
        
        controller = consumer._client.cluster.controller
        if controller:
            print(f"   Controller: Broker {controller.nodeId}")
        
        # Section 2: Topic Information
        print("\n2. TOPIC INFORMATION")
        print("-" * 40)
        
        topics = admin_client.list_topics()
        user_topics = [t for t in topics if not t.startswith("__")]
        internal_topics = [t for t in topics if t.startswith("__")]
        
        print(f"   User topics: {len(user_topics)}")
        print(f"   Internal topics: {len(internal_topics)}")
        
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
        
    except Exception as e:
        print(f"[ERROR] Could not explore cluster: {e}")
        return False


def create_demo_topic(bootstrap_servers: str = "localhost:9092"):

    print("\n" + "=" * 60)
    print("CREATING DEMO TOPIC")
    print("=" * 60)
    
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id="topic-creator"
        )
        
        # Define the topic
        topic = NewTopic(
            name="architecture-demo",
            num_partitions=3,
            replication_factor=1
        )
        
        # Create it
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"   Created topic 'architecture-demo' with 3 partitions")
        
        admin_client.close()
        return True
        
    except Exception as e:
        if "TopicAlreadyExistsError" in str(type(e).__name__):
            print(f"   Topic 'architecture-demo' already exists")
            return True
        print(f"[ERROR] Could not create topic: {e}")
        return False


def main():
    
    # Step 1: Explore existing cluster
    if not explore_cluster():
        print("\nEnsure Kafka is running: docker-compose up -d")
        sys.exit(1)
    
    # Step 2: Create a demo topic
    create_demo_topic()
    
    # Step 3: Explore again to see the new topic
    print("\n" + "=" * 60)
    print("UPDATED CLUSTER STATE")
    print("=" * 60)
    explore_cluster()
    
    print("\n" + "=" * 60)
    print("EXPLORATION COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()
