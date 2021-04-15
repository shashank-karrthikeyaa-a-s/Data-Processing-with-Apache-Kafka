# ----------------------------------------------------
# (C) Shashank Karrthikeyaa Annadanam Subbarathinam
# E-Mail : skas700@outlook.com
# GitHub : https://www.github.com/skas700
# ----------------------------------------------------

# Importing pyafka for connecting to kafka
from kafka import KafkaConsumer,KafkaProducer

# Importing JSON to convert JSON to Python Dictionary
import json

# Import SQLAlchemy
from sqlalchemy import create_engine

# Importing Pandas as Pd
import pandas as pd
# ----------------------------------------------------

# Configuration
# Server Address
server_address="ec2-3-238-29-77.compute-1.amazonaws.com"

# Kafka
kafka_subscribe_topic = "truck"
kafka_publish_topic = "truck_overspeed"

# Overspeed limit
overspeed_limit = 100

# ----------------------------------------------------
# Initializing Kafka Consumer
kafka_consumer = KafkaConsumer(kafka_subscribe_topic,bootstrap_servers = [server_address+':9092'])

# Initializing Kafka Producer
kafka_producer = KafkaProducer(bootstrap_servers = [server_address+':9092'])

# SQLAlchemy Engine
db_engine = create_engine("sqlite:///Database.db")

# Connection
db_Connection = db_engine.connect()

# Iterating through the messages
for message in kafka_consumer:

    #print(message.value)

    # Converting JSON to Python Dict
    message_dict = json.loads(message.value)

    # If Clause
    if message_dict["Speed"] > overspeed_limit:

        # Getting Truck Driver Details
        truck_name = message_dict["Truck"]

        # SQL Query
        query = '''
        SELECT
        *
        FROM
        TRUCK_DETAILS
        WHERE TRUCK_NAME = '{truck_name}'
        '''.format(truck_name = truck_name)

        # Executing the Query
        executed_query = db_Connection.execute(query)

        # Truck Driver Dictionary
        truck_driver_dict = [dict(data.items()) for data in executed_query][0]

        # print(truck_driver_dict)

        # Adding Over Speed Limit Value
        message_dict["Over Speed Limit"] = overspeed_limit

        # Merging both Dictionaries
        for key in truck_driver_dict.keys():
            message_dict[key] = truck_driver_dict[key]

        print("Over Speed")
        print("")

        # Overspeed Dictionary
        overspeed_json = json.dumps(message_dict)

        # Converting to Bytes
        overspeed_json = bytes(overspeed_json,'utf-8')
        print(overspeed_json)

        # Forwarding message to Kafka Producer
        kafka_producer.send(kafka_publish_topic,overspeed_json)