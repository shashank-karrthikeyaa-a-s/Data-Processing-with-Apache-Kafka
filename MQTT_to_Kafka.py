# ----------------------------------------------------
# (C) Shashank Karrthikeyaa Annadanam Subbarathinam
# E-Mail : skas700@outlook.com
# GitHub : https://www.github.com/skas700
# ----------------------------------------------------

# Importing Required Packages

#IMporting PyYaml to read the truck Configuration
import yaml

# Importing Sys,OS Packages for System functionalities
import sys,os

# Importing MQTT Client
import paho.mqtt.client as mqtt

# Importing pyafka for connecting to kafka
from kafka import KafkaProducer

# ----------------------------------------------------

# Configuration
# Server Address
server_address="ec2-3-238-29-77.compute-1.amazonaws.com"

# MQTT
mqtt_broker_subscribing_url="truck"
mqtt_broker_publishing_url="truck"
mqtt_node_id = "Server"


# Kafka
kafka_publish_topic = "truck"

# ----------------------------------------------------
# MQTT Functions

# Function to Log
def on_log(client,userdata,level,buf):
    pass

# Function to be executed when connected to the Broker
def on_connect(client,userdata,flags,rc):
    if rc==0:
        print("Connected OK")
        client.subscribe(mqtt_broker_subscribing_url)
    else:
        print("Bad connection Returned code=",rc)

# Function to be executed on Disconnection
def on_disconnect(client,userdata,flags,rc=0):
    print("DisConnected result code"+str(rc))

# Function to be executed when a mesage has arrived at the Subscribed Channel
def on_message(client,userdata,msg):
    topic=msg.topic
    m_decode=str(msg.payload.decode("utf-8"))
    print(topic,m_decode)

    # Inheriting Global Variables
    global kafka_producer
    global kafka_publish_topic

    # Forwarding message to Kafka Producer
    kafka_producer.send(kafka_publish_topic,msg.payload)

    # Initializinf File
    file = open("Raw Data Storage.txt","a")

    file.write(m_decode)

    file.close()

# ----------------------------------------------------

# Initializing Kafka Producer
kafka_producer = KafkaProducer(bootstrap_servers = [server_address+':9092'])

# Initializing MQTT Client
mqtt_client=mqtt.Client(mqtt_node_id,clean_session=True)

# Assigning Callback FUnction on Connecting
mqtt_client.on_connect=on_connect

# Assigning Callback FUnction for Logging
mqtt_client.on_log=on_log

# Assigning Callback FUnction on Disconnecting
mqtt_client.on_disconnect=on_disconnect

# Assigning Callback FUnction to process received messages
mqtt_client.on_message=on_message

# Printing Broker Address
print("Connecting to broker",server_address)

# Connecting to the Broker
mqtt_client.connect(server_address)

# Looping and Sending Truck Information
while(1):
    mqtt_client.loop()

# Disconnecting from the Broker
mqtt_client.disconnect()