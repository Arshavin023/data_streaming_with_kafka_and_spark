from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import uuid
import threading
from datetime import datetime
import json
import time
import random
# from ..src import logger
import os
import sys 
import logging

nigerian_states = [
    "Abia", "Adamawa", "Akwa Ibom", "Anambra", "Bauchi", "Bayelsa", "Benue",
    "Borno", "Cross River", "Delta", "Ebonyi", "Edo", "Ekiti", "Enugu", "Gombe",
    "Imo", "Jigawa", "Kaduna", "Kano", "Katsina", "Kebbi", "Kogi", "Kwara",
    "Lagos", "Nasarawa", "Niger", "Ogun", "Ondo", "Osun", "Oyo", "Plateau",
    "Rivers", "Sokoto", "Taraba", "Yobe", "Zamfara"
]

logging_str = "[%(asctime)s: %(levelname)s: %(module)s: %(message)s]"
log_dir = "logs"
log_filepath = os.path.join(log_dir,"running_logs.log")
os.makedirs(log_dir,exist_ok=True)

logging.basicConfig(
    level= logging.INFO,
    format= logging_str,

    handlers= [
        logging.FileHandler(log_filepath),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger("KafkaSparkProject")

KAFKA_BROKERS="localhost:29092,localhost:39092,localhost:49092"
NUM_PARTITIONS = 5
REPLICATION_FACTOR = 3
TOPIC_NAME = 'financial_transactions'

producer_conf = {
    "bootstrap.servers": KAFKA_BROKERS,
    "queue.buffering.max.messages": 10000,
    "queue.buffering.max.kbytes": 512000,
    "batch.num.messages": 1000,
    "linger.ms": 10,
    "acks": 1,
    "compression.type": 'gzip'
}

producer = Producer(producer_conf)

def create_topic(topic_name):
    admin_client=AdminClient({"bootstrap.servers": KAFKA_BROKERS})

    try:
        metadata = admin_client.list_topics(timeout=10)
        if topic_name not in metadata.topics:
            topic = NewTopic(
                topic=topic_name,
                num_partitions=NUM_PARTITIONS,
                replication_factor=REPLICATION_FACTOR
            )
            fs = admin_client.create_topics([topic])
            for topic, future in fs.items():
                try:
                    future.result()
                    logger.info(f'topic {topic_name} successfully created')
                except Exception as e:
                    logger.info(f'failed to create topic {topic_name}: {e}')
        else:
            logger.info(f'topic: {topic_name} already exists')

    except Exception as e:
        logger.info(f'Error ecountered while creating Topic: {e}')

def generate_transaction():
    return dict(
        transactionId=str(uuid.uuid4()),
        userId=f"user_{random.randint(1, 100)}",
        amount=round(random.uniform(50000, 500000), 2),
        transactionTime=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        merchantId=random.choice(['OPAY','MONIEPOINT','PALMPAY','OKCash']),
        transaction_type = random.choice(['purchase','refund']),
        location = random.choice(nigerian_states),
        payment_method = random.choice(['POS/CreditCard','Cash','USSD Transfer','Online Banking','3rd-Party OnlineBanking']),
        international_transaction = random.choice([True, False]),
        currency = random.choice(['NGN','USD','GBP','YEN','EUR'])
    )

def msg_delivery_report(err, msg):
    if err is not None:
        logger.info(f'Delivery failed for record {msg.key()}')
    else:
        logger.info(f'Record {msg.key()} successfully produced')

def produce_transaction(thread_id, num_transactions):
    # Produce the given number of transactions per thread
    for _ in range(num_transactions):
        transaction = generate_transaction()
        try:
            producer.produce(
                topic=TOPIC_NAME,
                key=transaction['userId'],
                value=json.dumps(transaction).encode('utf-8')
            )
            logger.info(f' Thread {thread_id} produced transaction: {transaction}')
            producer.flush()
        except Exception as e:
            logger.info(f'Error sending transaction from thread {thread_id}: {e}')

def parallel_producer(num_threads: int, transactions_per_thread:int, total_transactions: int):

    threads = []

    while total_transactions > 0:
        try:
            # Adjust the number of transactions per thread if not enough transactions left
            for i in range(num_threads):
                # Determine how many transactions to give this thread
                transactions_to_produce = min(transactions_per_thread, total_transactions)
                
                thread = threading.Thread(target=produce_transaction, args=(i, transactions_to_produce))
                thread.daemon = True
                thread.start()
                threads.append(thread)

            # Wait for all threads to finish before starting the next batch
            for thread in threads:
                thread.join()

            # Decrement the remaining transactions by the total number of transactions produced in this batch
            total_transactions -= transactions_per_thread * num_threads

        except Exception as e:
            logger.info(f'Error producing parallel transactions: {e}')


if __name__=="__main__":
    create_topic(TOPIC_NAME)
    parallel_producer(5,100000,1000000)

