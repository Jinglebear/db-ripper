from kafka import KafkaConsumer
from utility import Utils
import json
import requests
from datetime import datetime, timedelta

#logging
#print("Started\n")

# save incoming json on elasticsearch
def save_on_elasticsearch(parkingSpaceJson, id):
    # connect to elasticsearch with default config
    _es = Utils.connect_elasticsearch()
    if (_es == None):
        return
    
    # create index if not exists with defaultname
    Utils.create_index(_es)

    try:
        _es.index(Utils.esIndex, body=parkingSpaceJson)
    except Exception as e:
        print('Error in indexing data')
        print(str(e))


# fetch space data, create json and save on elasticsearch
def extract_space_data(response):

    allocations = response['allocations']

    for allocation in allocations:
        parkingInformation = {}

        if(len(allocation["allocation"]) == 6):
            allocation_id = allocation['space']['id']
            allocation_station_name = allocation['space']['station']['name']
            allocation_category = allocation['allocation']['category']
            
            parkingInformation['spaceID'] = allocation_id
            parkingInformation['stationName'] = allocation_station_name
            parkingInformation['parkingCategory'] = allocation_category

            parkingInformation['event'] = 'parking'

            #create timestamp for elastic search
            currentDT = datetime.now() + timedelta(hours=1)
            currentDT_formated = currentDT.strftime("%y%m%d%H%M")
            #add formatted timestamp to parkingInformation JSON Object
            parkingInformation['timestamp'] = currentDT_formated
            #write JSON object on elasticsearch
            save_on_elasticsearch(json.dumps(parkingInformation,indent=4),id=allocation_id)

consumer = KafkaConsumer(Utils.topicParkingTimetables, group_id='db_ripper',bootstrap_servers=Utils.bootstrap_servers)

for message in consumer:
    messageValue = message.value
    messageValueAsString = messageValue.decode('utf-8').replace("'",'"')
    extract_space_data(json.loads(messageValueAsString))

    