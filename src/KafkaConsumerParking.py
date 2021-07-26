import csv
import sys
import ast
from datetime import datetime, timedelta
try:
    from kafka import KafkaConsumer
    from utility import Utils
    import json
    import requests
except Exception as e:
    print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaConsumerParking: Exception by import", e, file=sys.stderr)


# save incoming json on elasticsearch
def save_on_elasticsearch(parkingSpaceJson, id):
    # connect to elasticsearch with default config
    _es = Utils.connect_elasticsearch()
    if (_es == None):
        print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaConsumerParking: Can't connect to elasticsearch", file=sys.stderr)
        return
    
    # create index if not exists with defaultname
    Utils.create_index(_es)

    try:
        _es.index(Utils.esIndex, body=parkingSpaceJson)
    except Exception as e:
        print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaConsumerParking: Error while indexing data.", e, file=sys.stderr)

import csv
def readStationData(filename):
    station_data =[]
    with open(filename,encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            station_data.append(row)
    return station_data

#function call to get station_data (stationName,stationCategory,Coordinates)
station_data=readStationData("/home/nils/db-ripper/misc/test_table_result.csv")

#para = stationName
#ret = location array [long,lat]
#lookup coordinates from list
def get_Location(stationName,station_data):
    for station in station_data:
        if(station[0] == stationName):
            coordinates_data=ast.literal_eval(station[3])
            coordinates = coordinates_data.get("coordinates")
            return coordinates
    

# fetch space data, create json and save on elasticsearch
def extract_space_data(response):

    allocations = response['allocations']

    for allocation in allocations:
        parkingInformation = {}

        if(len(allocation["allocation"]) == 6):
            allocation_id = allocation['space']['id']
            #trainstation name
            allocation_station_name = allocation['space']['station']['name']
            allocation_category = allocation['allocation']['category']
            
            
            parkingInformation['spaceID'] = allocation_id
            parkingInformation['stationName'] = allocation_station_name
            parkingInformation['parkingCategory'] = allocation_category
            parkingInformation['event'] = 'parking'
            ##array consisting of long and lat
            parkingInformation['location']= get_Location(allocation_station_name,station_data=station_data)
            #create timestamp for elastic search
            currentDT = datetime.now() + timedelta(hours=1)
            currentDT_formated = currentDT.strftime("%y%m%d%H%M")
            #add formatted timestamp to parkingInformation JSON Object
            parkingInformation['timestamp'] = currentDT_formated
            #write JSON object on elasticsearch
            parkingInformation_asJson = json.dumps(parkingInformation,indent=5)
            print(parkingInformation_asJson)
            save_on_elasticsearch(json.dumps(parkingInformation,indent=5),id=allocation_id)

consumer = KafkaConsumer(Utils.topicParkingTimetables, group_id='db_ripper',bootstrap_servers=Utils.bootstrap_servers)

for message in consumer:
    messageValue = message.value
    messageValueAsString = messageValue.decode('utf-8').replace("'",'"')
    extract_space_data(json.loads(messageValueAsString))

    