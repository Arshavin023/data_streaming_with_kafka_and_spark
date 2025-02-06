from pyspark.sql import SparkSession 
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType

KAFKA_BROKERS = 'kafka-broker-1:19092,kafka-broker-2:19092,kafka-broker-3:19092' #'localhost:29092,localhost:39092,localhost:49092'
SOURCE_TOPIC = 'financial_transactions'
AGGREGATES_TOPIC = 'transaction_aggregates'
ANOMALIES_TOPIC = 'transaction_anomalies'
CHECKPOINT_DIR = '/mnt/spark-checkpoints'
STATES_DIR = '/mnt/spark-state'

spark = (SparkSession.builder
    .appName("FinancialTransactionProcessor")
    .config('spark-jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0')
    .config('spark.sql.streaming.checkpointLocation', CHECKPOINT_DIR)
    .config('spark.sql.streaming.stateStore.stateStoreDir',STATES_DIR)
    .config('spark.sql.shuffle.partitiosn',20)
    .config('spark.sql.streaming.kafka.consumer.pollTimeoutMs', 300)  # Increase timeout to 30 seconds
    .config('spark.kafka.consumer.timeout.ms', 300)  # Increase Kafka consumer timeout
    ).getOrCreate()

spark.sparkContext.setLogLevel("WARN")
# spark.sparkContext.setLogLevel("DEBUG")

transaction_schema = StructType([
    StructField('transactionId', StringType(), True),
    StructField('userId', StringType(), True),
    StructField('amount', DoubleType(), True),
    StructField('transactionTime', TimestampType(), True),
    StructField('merchantId', StringType(), True),
    StructField('transactionType', StringType(), True),
    StructField('location', StringType(), True),
    StructField('paymentMethod', StringType(), True),
    StructField('isInternationalTransaction', BooleanType(), True),
    StructField('currency', StringType(), True)

])

kafka_stream = (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers', KAFKA_BROKERS)
                .option('subscribe', SOURCE_TOPIC)
                .option('startingOffsets','earliest')
                .option('failOnDataLoss', 'false')
                .load()
)

print('connected to kafka stream')

transaction_df = kafka_stream.selectExpr("CAST(value AS STRING)") \
                .select(from_json(col('value'), transaction_schema).alias('data')) \
                .select('data.*')


agg_merchant_df = transaction_df.groupby('merchantId') \
                        .agg(
                            sum('amount').alias('totalAmount'),
                            count('transactionId').alias('transactionCount')
                             )
print('agg_merchant_df created')

agg_merchant_query = agg_merchant_df.withColumn("key",col('merchantId').cast('string')) \
        .withColumn('value', to_json(struct(
            col('merchantId'),
            col('totalAmount'),
            col('transactionCount')
        ))).selectExpr('key','value') \
        .writeStream \
        .format('kafka') \
        .outputMode('update') \
        .option('failOnDataLoss', 'false') \
        .option('kafka.bootstrap.servers',KAFKA_BROKERS) \
        .option('topic',AGGREGATES_TOPIC) \
        .option('checkpointLocation', f'{CHECKPOINT_DIR}/aggregates') \
        .start().awaitTermination()
