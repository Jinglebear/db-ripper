import sys
from datetime import datetime
from utility import Utils

try:
    from kafka import KafkaConsumer
    import xml.etree.ElementTree as ET
    import time
    import json
    import threading
except Exception as e:
    Utils.print_error("KafkaConsumerChanges", "Error while import - " + e)

# message code dictionary
code_dict = {
    "0": "keine Verspätungsbegründung",
    "2": "Polizeiliche Ermittlung",
    "3": "Feuerwehreinsatz an der Strecke",
    "4": "kurzfristiger Personalausfall",
    "5": "ärztliche Versorgung eines Fahrgastes",
    "6": "Notbremse",
    "7": "Personen im Gleis"
}

def get_from_elasticsearch(id):
    # connect to elasticsearch
    _es = Utils.connect_elasticsearch()
    if _es == None:
        Utils.print_error("KafkaConsumerChanges", "Can't connect to elasticsearch")
        return None
    
    # create index
    Utils.create_index(_es)

    # get from elasticsearch with timeout
    count = 0
    while(count < 15):
        query = {"query":{"match":{"id":{"query":id,"operator": "and"}}}}
        es_object = _es.search(index=Utils.es_default_index, body=query)
        if es_object == None or len(es_object['hits']['hits'])!=1:
            time.sleep(1)
            count += 1
        else:
            return es_object['hits']['hits'][0]['_source']
    Utils.print_error("KafkaConsumerChanges", "can't find" + id + "in elasticsearch")
    return None

def save_on_elasticsearch(dictionary, dic_id):
    # get connection to elasticsearch
    _es = Utils.connect_elasticsearch()
    if _es == None:
        Utils.print_error("KafkaConsumerChanges", "Can't connect to elasticsearch")
        return None

    # update entry
    source_to_update = {
        "doc": dictionary
    }
    # print (source_to_update)
    object_json = json.dumps(source_to_update)
    _es.update(index=Utils.es_default_index, id=dic_id, doc_type="_doc", body=object_json)

# extraction from element tree and prefix a time different
def extract_time_diff(xml_element_tree, plan, prefix):
    # proof exist of the tag in element tree
    time_tag = xml_element_tree.find(prefix)
    if time_tag:
        # get time value of the tag
        time = time_tag.attrib['ct']
        # calculate time diff
        time_diff = int(time) - int(plan[prefix+'Time'])
        # time diff must be positiv
        if time_diff < 0:
            time_diff = time_diff * -1
        # avoid calculation errors during day change
        if time_diff >= 7640:
            time_diff = time_diff - 7640
        # save time diff in plan object
        plan[prefix+'TimeDiff']

# add message of code in plan
def add_code(message, plan, name):
    code = message.get('c')
    code_string = code_dict[str(code)]
    if code_string:
        if plan.get(name) == None:
            plan[name] = []
        if code_string not in plan.get(name):
            plan[name].append(code_string)

# factorize xml from kafka
def factorize_message(xml_string):
    root = ET.fromstring(xml_string)

    for s in root: 
        try:
            # get planned from elasticsearch
            sid = s.attrib['id']
            if "-" not in sid:
                continue
            plan = get_from_elasticsearch(sid)
            if plan == None:
                continue

            # extract Information and extend planned data
            # time diff of arrive and departure
            extract_time_diff(s, plan, 'ar')
            extract_time_diff(s, plan, 'dp')

            # append message code
            for message in s.findall('.//m'):
                
                t = message.get('t')
                if t == None:
                    continue

                if t == "q":
                    add_code(name = "qualityDevation", message = message, plan = plan)
                elif t == "d":
                    add_code(name = "reasonForDelay", message = message, plan = plan)

        except Exception as e:
            Utils.print_error("KafkaConsumerChanges", "Error in processing kafka message -" + e)
            
        try:
            # update plan object in elasticsearch
            save_on_elasticsearch(plan, sid)
        except Exception as e:
            Utils.print_error("KafkaConsumerChanges", "Error while save on elasticsearch - " + e)
            
## Work
Utils.print_log("KafkaConsumerChanges", "start consumer")
consumer = KafkaConsumer(Utils.topic_timetable_changed, group_id='db_ripper', bootstrap_servers=Utils.bootstrap_servers)

for message in consumer:
    message_value = message.value
    message_value_as_string = message_value.decode('utf-8')
    thread = threading.Thread(target=factorize_message, args=(message_value_as_string,))
    thread.start()