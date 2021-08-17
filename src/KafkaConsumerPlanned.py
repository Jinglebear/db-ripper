
from utility import Utils

try:
    import threading
    from kafka import KafkaConsumer
    import xml.etree.ElementTree as ET
    import json
    import sys
    from datetime import datetime, timedelta
except Exception as e:
    Utils.print_error("KafkaConsumerPlannes", "Error while import", e)

# save incoming json on elasticsearch
def save_on_elasticsearch(timetable_json, id):
    # connect to elasticsearch with default config
    es = Utils.connect_elasticsearch()
    if es == None:
        Utils.print_error("KafkaConsumerPlanned", "Can't connect to elasticsearch")
        return

    # create index if not exists with defaultname
    Utils.create_index(es)

    # store json on elasticsearch
    try:
        # write on elasticsearch
        es.index(Utils.es_default_index, body=timetable_json, id=id)
    except Exception as e:
        Utils.print_error("KafkaConsumerPlanned", "Error while indexing data", e)
        


# factorize incoming message in a thread
# create json and give them to save_on_elasticsearch
def factorize_message(xml_string):
    root = ET.fromstring(xml_string)

    try:
        # extract trainStation out of xml
        train_station = root.attrib['station']
    except:
        # timetable is empty
        # possible when no train in this hourslice is planned
        error=""

    # child of timetable are event with tag "s"
    for s in root:
        try:
            # save values in trainInformation
            train_information = {}

            # location coordinates with lon and lan for elasticsearch
            city_info = Utils.get_city_info(train_station)
            train_information['location'] = city_info.get('location')
            train_information['city'] = city_info.get('cityname')
            # timestamp for elasticsearch
            train_information['timestamp'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

            train_information['station'] = train_station

            train_information['event'] = 'timetable'
            train_information['id'] = s.attrib['id']
            trip_label = s.find('tl')
            train_information['trainType'] = trip_label.attrib.get('f')
            train_information['trainCategory'] = trip_label.attrib.get('c')

            # extract arriveTime if arrive event exist
            arrive = s.find('ar')
            if arrive != None:
                train_information['arTime'] = arrive.attrib.get('pt')

            # extract departureTime if departure event exist
            departure = s.find('dp')
            if departure != None:
                train_information['dpTime'] = departure.attrib.get('pt')

            # transform dictinary to json and save on elasticsearch
            json_object = json.dumps(train_information)
            save_on_elasticsearch(json_object, train_information['id'])
        except Exception as e:
            Utils.print_error("KafkaConsumerPlanned", "Error while extracting data from kafka message", e)

## Work
Utils.print_log("KafkaConsumerPlanned", "start consumer")
consumer = KafkaConsumer(Utils.topic_timetable_planned, group_id='db_ripper' , bootstrap_servers=Utils.bootstrap_servers)

for message in consumer:
    message_value = message.value
    message_value_as_string = message_value.decode('utf-8')
    thread = threading.Thread(target=factorize_message, args=(message_value_as_string,))
    thread.start()
