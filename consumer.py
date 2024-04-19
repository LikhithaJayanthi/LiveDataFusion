from pymongo import MongoClient
from kafka import KafkaConsumer
import json
import os

# Set the Kafka topic name
kafka_topic = "quickstart-events"

# Set the Kafka bootstrap servers
kafka_bootstrap_servers = "192.168.14.14:9092"

# Set up MongoDB connection
mongo_uri = "mongodb+srv://farsim:database@mycluster.thlwhor.mongodb.net/"
db_name = 'kafka_stream'
collection_name = 'kafka_data'

# Connect to MongoDB
client = MongoClient(mongo_uri)
db = client[db_name]
collection = db[collection_name]

# Create a Kafka consumer
consumer = KafkaConsumer(kafka_topic, bootstrap_servers=kafka_bootstrap_servers, value_deserializer=lambda v: json.loads(v.decode('utf-8')))

# Continuously poll for new messages
for message in consumer:
    # Get the JSON document from the Kafka message
    document = message.value

    # Insert the document into MongoDB
    collection.insert_one(document)

    # Print the inserted document
    print(f"Inserted: {document}")

# Close the Kafka consumer to release resources (this part will not be reached unless you manually stop the script)
consumer.close()
