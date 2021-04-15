# ----------------------------------------------------
# (C) Shashank Karrthikeyaa Annadanam Subbarathinam
# E-Mail : skas700@outlook.com
# GitHub : https://www.github.com/skas700
# ----------------------------------------------------

# Importing pyafka for connecting to kafka
from kafka import KafkaConsumer,KafkaProducer

# Importing JSON to convert JSON to Python Dictionary
import json

# Importing Pandas
import pandas as pd

# Importing Datetime
from datetime import datetime

# ----------------------------------------------------
# Configuration
# Server Address
server_address="ec2-3-238-29-77.compute-1.amazonaws.com"

# Kafka
kafka_subscribe_topic = "truck"
kafka_publish_topic = "truck_summary"

# Send Summary Time Seconds
send_summary_time_intreval_seconds = 120

# Overspeed limit
overspeed_limit = 100

# ----------------------------------------------------
# Initializing Dataframe for daily events
data_df = pd.DataFrame()

# Initializing Time Metric
last_updated_time = datetime(1997,1,1)

# ----------------------------------------------------
# Initializing Kafka Consumer
kafka_consumer = KafkaConsumer(kafka_subscribe_topic,bootstrap_servers = [server_address+':9092'])

# Initializing Kafka Producer
kafka_producer = KafkaProducer(bootstrap_servers = [server_address+':9092'])

# Iterating through the messages
for message in kafka_consumer:

    # Converting JSON to Python Dict
    message_dict = json.loads(message.value)

    # Converting Dictionary to Pandas Data Frame
    message_df = pd.DataFrame(message_dict,index = [0])

    # Appending to the DataFrame
    data_df = data_df.append(other = message_df,ignore_index = True).reset_index(drop = True)

    if (datetime.now()-last_updated_time).total_seconds()>send_summary_time_intreval_seconds :

        # Engine Overspeed element
        data_df["Over Speed"] = data_df["Speed"].apply(lambda x: 1 if x >= overspeed_limit else 0)

        
        # Grouping Information based on the Truck
        grouped_df = data_df.groupby(by=["Truck"]).agg({"Time Stamp":["min","max"],
                                            "Latitude":["min","max"],
                                            "Longitude":["min","max"],
                                            "Speed":["min","max"],
                                            "Over Speed":["sum"]})


        #print(grouped_df)

        # Converting to Dictionary
        data_dict = grouped_df.to_dict()

        # Initializing New Dictionary
        data_dict_cleaned = {}

        # Iterating through the keys
        for key in data_dict.keys():
            new_key = " ".join(key)
            data_dict_cleaned[new_key] = data_dict[key]

        # Overspeed Dictionary
        summary_json = json.dumps(data_dict_cleaned)

        # Converting to Bytes
        summary_json = bytes(summary_json,'utf-8')

        print("")
        print(summary_json)

        # Forwarding message to Kafka Producer
        kafka_producer.send(kafka_publish_topic,summary_json)