from os import EX_CANTCREAT
import threading
from kafka import KafkaConsumer
import xml.etree.ElementTree as ET
import json
from Utility import Utils
# save incoming json on elasticsearch
def save_on_elasticsearch(timetableJson, id):
    # connect to elasticsearch with default config
    _es = Utils.connect_elasticsearch()
    if _es == None:
        return

    # create index if not exists with defaultname
    Utils.create_index(_es)

    # store json on elasticsearch
    try:
        _es.index(Utils.esIndex, body=timetableJson, id=id)
    except Exception as e:
        print('Error in indexing data')
        print(str(e))


# factorize incoming message in a thread
# create json and give them to save_on_elasticsearch
def factorize_message(xmlString):
    root = ET.fromstring(xmlString)

    # extract trainStation out of xml
    try:
        trainStation = root.attrib['station']

        # child of timetable are event with tag "s"
        for s in root:
            # save values in trainInformation
            trainInformation = {}
            trainInformation['station'] = trainStation

            trainInformation['event'] = 'timetable'
            trainInformation['id'] = s.attrib['id']
            tripLabel = s.find('tl')
            trainInformation['trainType'] = tripLabel.attrib.get('f')
            trainInformation['trainCategory'] = tripLabel.attrib.get('c')

            # extract arriveTime if arrive event exist
            arrive = s.find('ar')
            if arrive != None:
                trainInformation['arTime'] = arrive.attrib.get('pt')

            # extract departureTime if departure event exist
            departure = s.find('dp')
            if departure != None:
                trainInformation['dpTime'] = departure.attrib.get('pt')

            # transform dictinary to json and save on elasticsearch
            json_object = json.dumps(trainInformation)
            save_on_elasticsearch(json_object, trainInformation['id'])
    except:
        print(xmlString)

def KafkaConsumerPlannedMain():
    consumer = KafkaConsumer(Utils.topicForPlannedTimetables, group_id='db_ripper' , bootstrap_servers=Utils.bootstrap_servers)

    for message in consumer:
        messageValue = message.value
        messageValueAsString = messageValue.decode('utf-8')
        thread = threading.Thread(target=factorize_message, args=(messageValueAsString,))
        thread.start()

KafkaConsumerPlannedMain()