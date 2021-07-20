import sys
from datetime import datetime

try:
    import threading
    from kafka import KafkaConsumer
    import xml.etree.ElementTree as ET
    import json
    from utility import Utils
except Exception as e:
    print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaConsumerPlanned: Exception by import", e, file=sys.stderr)

# save incoming json on elasticsearch
def save_on_elasticsearch(timetableJson, id):
    # connect to elasticsearch with default config
    _es = Utils.connect_elasticsearch()
    if _es == None:
        print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaConsumerPlanned: Can't connect to elasticsearch", file=sys.stderr)
        return

    # create index if not exists with defaultname
    Utils.create_index(_es)

    # store json on elasticsearch
    try:
        response = _es.index(Utils.esIndex, body=timetableJson, id=id)
    except Exception as e:
        print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaConsumerPlanned: Error while indexing data.", e, file=sys.stderr)
        


# factorize incoming message in a thread
# create json and give them to save_on_elasticsearch
def factorize_message(xmlString):
    root = ET.fromstring(xmlString)

    try:
        # extract trainStation out of xml
        trainStation = root.attrib['station']
    except:
        # timetable is empty
        # possible when no train in this hourslice is planned
        error=""

    # child of timetable are event with tag "s"
    for s in root:
        try:
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
        except Exception as e:
            print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaConsumerPlanned: Error in an event", s.getchildren(), file=sys.stderr)

## Work
print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "KafkaConsumerPlanned: start consumer")
consumer = KafkaConsumer(Utils.topicForPlannedTimetables, group_id='db_ripper' , bootstrap_servers=Utils.bootstrap_servers)

for message in consumer:
    messageValue = message.value
    messageValueAsString = messageValue.decode('utf-8')
    thread = threading.Thread(target=factorize_message, args=(messageValueAsString,))
    thread.start()
