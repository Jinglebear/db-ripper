from kafka import KafkaConsumer
from Utility import Utils
import xml.etree.ElementTree as ET
import time
import json
import threading

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
        query = {"query":{"match":{"id":{"query":id,"operator": "and"}}}}
        object = _es.search(index=Utils.esIndex, body=query)
        if object == None or len(object['hits']['hits'])!=1:
            time.sleep(1)
            count += 1
        else:
            return object['hits']['hits'][0]['_source']
    return None

def save_on_elasticsearch(dictionary, dicId):
    
    
    # get connection to elasticsearch
    _es = Utils.connect_elasticsearch()
    if _es == None:
        return None
    
    # index is created by get_from_elasticsearch

    # update entry
    source_to_update = {
        "doc": dictionary
    }
    # print(source_to_update)
    objectJson = json.dumps(source_to_update)
    response = _es.update(index=Utils.esIndex, id=dicId, doc_type="_doc", body=objectJson)
    print(response)



def factorize_message(xmlString):
    root = ET.fromstring(xmlString)

    for s in root: 
        # get planned from elasticsearch
        sid = s.attrib['id']
        plan = get_from_elasticsearch(sid)
        if plan == None:
            continue

        # extract Information and extend planned data
        # arTimeDiff
        arrive = s.find('ar')
        if arrive:
            arriveTime = arrive.attrib['ct']
            try:
                timeDiff = int(arriveTime) - int(plan['arTime'])
                if timeDiff < 0:
                    timeDiff = timeDiff * -1
                plan['arTimeDiff'] = timeDiff
            except Exception as e:
                print(arriveTime, e)

        # dpTimeDiff
        departure = s.find('ar')
        if departure:
            departureTime = departure.attrib['ct']
            try:
                timeDiff = int(departureTime) - int(plan['dpTime'])
                if timeDiff < 0:
                    timeDiff = timeDiff * -1
                plan['dpTimeDiff'] = timeDiff
            except Exception as e:
                print(departureTime, e)    
        # append message code
        for message in s.findall('m'):
            t = message.get('t')
            if t == None:
                continue

            if t == "q":
                # qualityDevation
                code = message.get('c')
                if plan['qualityDevation'] == None:
                    plan['qualityDevation'] == []
                if code not in plan['qualityDevation']:
                    plan['qualityDevation'].append(code)

            elif t == "d":
                # reason for delay
                code = message.get('c')
                if plan['reasonForDelay'] == None:
                    plan['reasonForDelay'] == []
                if code not in plan['reasonForDelay']:
                    plan['reasonForDelay'].append(code)
        save_on_elasticsearch(plan, sid)
            

def KafkaConsumerChangesMain():
    print("start KafkaConsumerChanges")
    consumer = KafkaConsumer(Utils.topicForChangedTimetabled, group_id='db_ripper', bootstrap_servers=Utils.bootstrap_servers)

    for message in consumer:
        messageValue = message.value
        messageValueAsString = messageValue.decode('utf-8')
        thread = threading.Thread(target=factorize_message, args=(messageValueAsString,))
        thread.start()

KafkaConsumerChangesMain()