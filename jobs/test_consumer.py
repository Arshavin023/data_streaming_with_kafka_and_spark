from confluent_kafka import Consumer, KafkaException, KafkaError

# Kafka configuration
KAFKA_BROKERS = 'localhost:29092,localhost:39092,localhost:49092'  # Update to your broker addresses
SOURCE_TOPIC = 'financial_transactions'  # The topic you want to test
GROUP_ID = 'test-group'  # Consumer group ID for the consumer

# Create a Kafka Consumer instance
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKERS,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest'  # Start consuming from the earliest offset
})

def test_kafka_connectivity():
    try:
        # Subscribe to the topic
        consumer.subscribe([SOURCE_TOPIC])
        
        # Poll for a message
        msg = consumer.poll(timeout=10.0)  # Wait for up to 10 seconds for a message
        
        if msg is None:
            print("No message received within the timeout. Kafka connectivity seems fine, but no message.")
        elif msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("End of partition reached. No more messages to consume.")
            else:
                print(f"Error while consuming message: {msg.error()}")
        else:
            print(f"Message consumed: {msg.value().decode('utf-8')}")
    
    except KafkaException as e:
        print(f"Kafka exception occurred: {e}")
    # finally:
    #     # Close the consumer connection
    #     consumer.close()

if __name__ == "__main__":
    while True:
        test_kafka_connectivity()
