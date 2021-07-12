from kafka import KafkaConsumer
from Utility import Utils
import xml.etree.ElementTree as ET
import time
import json

def get_from_elasticsearch(id):
    # connect to elasticsearch
    _es = Utils.connect_elasticsearch()
    if _es == None:
        return None
    
    # create index
    Utils.create_index(_es)

    # get from elasticsearch with timeout
    count = 0
    while(count < 15):
        object = _es.search(index=Utils.esIndex, id=id)
        if object == None:
            time.sleep(1)
            count += 1
        else:
            return object
    return None

def factorize_message(xmlString):
    root = ET.fromstring(xmlString)

    for s in root: 
        # get planned from elasticsearch
        sid = s.attrib['id']
        planJson = get_from_elasticsearch(sid)
        if plan == None:
            continue

        plan = json.load(planJson)

        # extract Information and extend planned data
        # arTimeDiff
        arrive = s.find('ar')
        if arrive:
            arriveTime = arrive.attrib['ct']
            timeDiff = int(arriveTime) - plan['arTime']
            if timeDiff < 0:
                timeDiff = timeDiff * -1
            plan['arTimeDiff'] = timeDiff

        # dpTimeDiff
        departure = s.find('ar')
        if departure:
            departureTime = departure.attrib['ct']
            timeDiff = int(departureTime) - plan['dpTime']
            if timeDiff < 0:
                timeDiff = timeDiff * -1
            plan['dpTimeDiff'] = timeDiff

        # qualityDevation
        for message in s.findAll('m'):
            m=1

        # reason for delay

consumer = KafkaConsumer(Utils.topicForChangedTimetabled, group_id='db_ripper', bootstrap_servers=Utils.bootstrap_servers)

for message in consumer:
    messageValue = message.value
    messageValueAsString = messageValue.decode('utf-8')
    