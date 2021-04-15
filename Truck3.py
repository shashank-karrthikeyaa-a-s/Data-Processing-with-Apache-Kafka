# ----------------------------------------------------
# (C) Shashank Karrthikeyaa Annadanam Subbarathinam
# E-Mail : skas700@outlook.com
# GitHub : https://www.github.com/skas700
# ----------------------------------------------------

# IMporting required Libraries

#IMporting PyYaml to read the truck Configuration
import yaml

# Importing Sys,OS Packages for System functionalities
import sys,os

# Importing Date Time for Date & Time Operation
from datetime import datetime

# Importing MQTT Client
import paho.mqtt.client as mqtt

# Converting to JSON
import json

# Importing random Library to generate Random Integers
from random import randint

# ----------------------------------------------------

# Mentioning Broker Subscription and Publishing URL

# Address / IP of the location where the MQTT Broker is running
broker_address="ec2-3-238-29-77.compute-1.amazonaws.com"

# URL to Subscribe
broker_subscribing_url="test"

# URL to Publish
broker_publishing_url="truck"

# Truck ID is the Node ID
node_id = "Truck3"

# ----------------------------------------------------

# MQTT Functions

# Function to Log
def on_log(client,userdata,level,buf):
    pass

# Function to be executed when connected to the Broker
def on_connect(client,userdata,flags,rc):
    if rc==0:
        print("Connected OK")
        client.subscribe(broker_subscribing_url)
    else:
        print("Bad connection Returned code=",rc)

# Function to be executed on Disconnection
def on_disconnect(client,userdata,flags,rc=0):
    print("DisConnected result code"+str(rc))

# Function to be executed when a mesage has arrived at the Subscribed Channel
def on_message(client,userdata,msg):
    topic=msg.topic
    m_decode=str(msg.payload.decode("utf-8"))
    print(m_decode)

# Function publishing test details
def publishing_command_test():
    client.publish(broker_publishing_url,"Test",qos=2)
        
# Function to Send Truck Location
def send_truck_location():

    global truck_last_sent_time
    global location_sent
    global location_list

    # Simulating Random Value of  Speed
    speed_random_val = randint(0,140)

    if truck_last_sent_time == None :
        
        print("1st Location")

        # Updating the Last Sent Time
        truck_last_sent_time = datetime.now()

        # Sending the First Location
        location_sent = 0

        # Getting the Latitude and Longitude from Location List
        latitude,longitude = location_list[location_sent].split(",")

        # Sending the required Information
        client.publish(broker_publishing_url,json.dumps(
                {"Truck":node_id,
                "Time Stamp":str(datetime.now()),
                "Latitude":latitude,
                "Longitude":longitude,
                "Speed":speed_random_val}))

        print("\n")
        print({"Truck":node_id,
                "Time Stamp":str(datetime.now()),
                "Latitude":latitude,
                "Longitude":longitude,
                "Speed":speed_random_val})

    elif (datetime.now() - truck_last_sent_time).total_seconds() > time_intreval:

        print("Location Sent")

        # Updating the Last Sent Time
        truck_last_sent_time = datetime.now()

        if location_sent < len(location_list)-1:

            # Sending the Next Location
            location_sent = location_sent+1

        else:
            
            # Sending the Next Location
            location_sent = 0

        # Getting the Latitude and Longitude from Location List
        latitude,longitude = location_list[location_sent].split(",")

        # Sending the required Information
        client.publish(broker_publishing_url,json.dumps(
                {"Truck":node_id,
                "Time Stamp":str(datetime.now()),
                "Latitude":latitude,
                "Longitude":longitude,
                "Speed":speed_random_val}))

        print("\n")
        print({"Truck":node_id,
                "Time Stamp":str(datetime.now()),
                "Latitude":latitude,
                "Longitude":longitude,
                "Speed":speed_random_val})

        #print(truck_last_sent_time,location_sent)

    #return truck_last_sent_time,location_sent,location_list


# ----------------------------------------------------

# MAIN

# Loading Truck Configuration as a Python Dictionary
truckConfigDict = yaml.safe_load(open(os.getcwd()+"/truckConfig.yaml"))

# Printing Truck Configuration Dictionary
#print(truckConfigDict)

# Time Intreval for which Messages should be sent
time_intreval = truckConfigDict[node_id]['Time_Intreval_Seconds']

# List of Locations
location_list = truckConfigDict[node_id]["Location"]

# ------------ 

# Global Variable Keeping Track of Location Point sent
location_sent = None

# Global Variable Keeping Track of Last sent time
truck_last_sent_time = None

# --------------

# Initializing MQTT Client
client=mqtt.Client(node_id,clean_session=True)

# Assigning Callback FUnction on Connecting
client.on_connect=on_connect

# Assigning Callback FUnction for Logging
client.on_log=on_log

# Assigning Callback FUnction on Disconnecting
client.on_disconnect=on_disconnect

# Assigning Callback FUnction to process received messages
client.on_message=on_message

# Printing Broker Address
print("Connecting to broker",broker_address)

# Connecting to the Broker
client.connect(broker_address)

# Looping and Sending Truck Information
while(1):
    client.loop()
    send_truck_location()

# Disconnecting from the Broker
client.disconnect()