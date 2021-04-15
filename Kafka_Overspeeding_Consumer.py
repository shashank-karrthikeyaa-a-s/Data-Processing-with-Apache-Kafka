# ----------------------------------------------------
# (C) Shashank Karrthikeyaa Annadanam Subbarathinam
# E-Mail : skas700@outlook.com
# GitHub : https://www.github.com/skas700
# ----------------------------------------------------

# Importing pyafka for connecting to kafka
from kafka import KafkaConsumer,KafkaProducer

# Importing JSON to convert JSON to Python Dictionary
import json

# ----------------------------------------------------

# Configuration
# Server Address
server_address="ec2-3-238-29-77.compute-1.amazonaws.com"

# Kafka
kafka_subscribe_topic = "truck_overspeed"

# ----------------------------------------------------

# Initializing Kafka Consumer
kafka_consumer = KafkaConsumer(kafka_subscribe_topic,bootstrap_servers = [server_address+':9092'])

# Iterating through the messages
for message in kafka_consumer:

    print("")
    print(message.value)