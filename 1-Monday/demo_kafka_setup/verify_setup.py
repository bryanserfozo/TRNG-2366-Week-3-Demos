import sys
import time
from kafka import KafkaAdminClient
from kafka.errors import NoBrokersAvailable

def check_kafka_connection(bootstrap_servers: str = "localhost:9092", max_retries : int = 5):
    # This method will allow us to check our connection to the Kafka Broker
    print(f"Attempting to connect to Kafka at {bootstrap_servers}")
    print( '-' * 60)

    for attempt in range(1, max_retries + 1):
        # We will create an object known as a KafkaAdminClient
        # This allows us to validate connection
        # If we are able to successfully create the object then we will have connected successfully
        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers = bootstrap_servers,
                client_id = 'setup-verification'
            )

            # If we make it to here, we'ree good to go
            print("[SUCCESS] Connected to Kafka Cluster")
            print(f'    Attempt: {attempt} / {max_retries}')

            # List existing topics to verify full functionality
            topics = admin_client.list_topics()
            print(f'    Topics found: {len(topics)}')

            if topics:
                print(' Existing Topics')
                for topic in topics:
                    if not topic.startswith("__"):
                        print(f'    - {topic}')

            admin_client.close()
            return True
        except NoBrokersAvailable:
            print(f'[ATTEMPT {attempt} / {max_retries}] No Brokers available, retrying in 5 seconds')
            time.sleep(5)
        except Exception as e:
            print(f'[ERROR] Unexpected error: {e}')
            time.sleep(5)

    return False

def get_cluster_metadata(bootstrap_servers: str = "localhost:9092"):
    """
    Retrieve and display cluster metadata.
    
    This demonstrates how to inspect the Kafka cluster configuration,
    which is useful for understanding the architecture in a live environment.
    """
    try:
        from kafka import KafkaConsumer
        
        # Using KafkaConsumer to access cluster metadata
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            client_id="metadata-viewer"
        )
        
        # Access the cluster metadata
        print("\n" + "=" * 50)
        print("CLUSTER METADATA")
        print("=" * 50)
        
        # Get broker information
        brokers = consumer._client.cluster.brokers()
        print(f"\nBrokers ({len(brokers)}):")
        for broker in brokers:
            print(f"  - Broker {broker.nodeId}: {broker.host}:{broker.port}")
        
        # Get controller information
        controller = consumer._client.cluster.controller
        if controller:
            print(f"\nController: Broker {controller.nodeId}")
        
        consumer.close()
        
    except Exception as e:
        print(f"[ERROR] Could not retrieve metadata: {e}")


def main():
    print('=' * 60)
    print('Kafka Setup Verification Demo')
    print('=' * 60)

    # First let's check the connection to our Kafka Server
    if check_kafka_connection():

        get_cluster_metadata()

        print("\n" + "=" * 60)
        print("Kafka Setup Completed - Kafka is ready for use!")
        print('=' * 60)
    else:
        print("\n" + "=" * 60)
        print("Kafka Setup failed!")
        print('=' * 60)
        sys.exit(1)
        


if __name__ == "__main__":
    main()