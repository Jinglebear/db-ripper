from utility import Utils
import csv

try:
    from kafka import KafkaConsumer
    from datetime import datetime, timedelta
    import sys
    import json
except Exception as e:
    Utils.print_error("KafkaConsumerParking Error while import:", e)


# save incoming json on elasticsearch
def save_on_elasticsearch(parking_space_json, id):
    # connect to elasticsearch with default config
    _es = Utils.connect_elasticsearch()
    if (_es == None):
        print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaConsumerParking: Can't connect to elasticsearch", file=sys.stderr)
        return
    
    # create index if not exists with defaultname
    Utils.create_index(_es)

    try:
        _es.index(Utils.es_default_index, body=parking_space_json)
    except Exception as e:
        Utils.print_error("KafkaConsumerParking: Error while indexing data" , e)


# fetch space data, create json and save on elasticsearch
def extract_space_data(response):
    
    allocations = response['allocations']

    for allocation in allocations:
        try:
            parking_information = {}

            if(len(allocation["allocation"]) == 6):
                allocation_id = allocation['space']['id']
               
                #trainstation name
                allocation_station_name = allocation['space']['station']['name']
                allocation_category = allocation['allocation']['category']
                
                
                parking_information['spaceID'] = allocation_id
                parking_information['stationName'] = allocation_station_name
                parking_information['parkingCategory'] = allocation_category
                parking_information['event'] = 'parking'
                
                ##array consisting of long and lat
                city_info = Utils.get_city_info(allocation_station_name)
                
                parking_information['location'] = city_info.get('location')
                parking_information['city'] = city_info.get('cityname')
                
                #create timestamp for elastic search
                current_dt = datetime.now()
                current_dt_formated = current_dt.strftime("%Y-%m-%dT%H:%M:%S")
                
                #add formatted timestamp to parkingInformation JSON Object
                parking_information['timestamp'] = current_dt_formated
                
                #write JSON object on elasticsearch
                save_on_elasticsearch(json.dumps(parking_information,indent=5),id=allocation_id)
            else:
                continue
        except Exception as e:
            Utils.print_error("KafkaConsumerParking : Error while extract_space_data:", e)

## Work
Utils.print_log("KafkaConsumerParking", "start consumer (parking)")       
consumer = KafkaConsumer(Utils.topic_parking, group_id='db_ripper',bootstrap_servers=Utils.bootstrap_servers)

for message in consumer:
    try:
        messageValue = message.value
        messageValueAsString = messageValue.decode('utf-8').replace("'",'"')
        extract_space_data(json.loads(messageValueAsString))
    except Exception as e:
        Utils.print_error("KafkaConsumerParking: Error in Consumer Loop:", e)

    