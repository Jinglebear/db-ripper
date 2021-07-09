from datetime import datetime
import threading
import time
from kafka import KafkaProducer
import requests 
from Utility import Utils

# load constants
authTokenList=Utils.tokenlistTimetable
headers = {
    'Accept': 'application/xml',
    'Authorization': authTokenList[0],
}
timeIntervalInSec = Utils.changeTimeInterval
topic = Utils.topicForChangedTimetabled
bootstrap_servers=Utils.bootstrap_servers

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

def send_on_success(record_metadata):
    print('topic:',record_metadata.topic,'partition:',record_metadata.partition)


# iterate over eva numbers and send response to kafka in a thread
def work_thread(producer, eva_numbers):
    calls_in_minute=0
    for eva in eva_numbers:
        if calls_in_minute < Utils.timetableInvocationLimit:
            calls_in_minute += 1
        else:
            time.sleep(60 - datetime.now().second)
            calls_in_minute = 0
        response = requests.get(Utils.get_changes_url(eva), headers=)
    
        producer.send(topic=topic, value=response.content).add_callback(send_on_success)
        
    producer.flush()


# Produce information end send to kafka
while True:
    # startTime
    start = datetime.now()

    # preparatory work: set hourslice and date
    hourSlice = start.hour
    # date in format: YYMMDD
    date = (str(start.year%1000) + 
        (('0'+str(start.month)) if (start.month<10) else (str(start.month))) + 
        (('0'+str(start.day)) if (start.day<10) else (str(start.day))))

    ##Work
    # load eva numbers
    evas = Utils.get_eva_numbers()
    # load tokens
    tokens = Utils.tokenlistTimetable
    # eva numbers that one token will process
    evas_per_token = int(len(evas) / len(tokens)) + 1
    # divide work on token
    for x in range(len(tokens)):
        thread = threading.Thread(target=work_thread, args=(evas[x*evas_per_token:(x+1)*evas_per_token], hourSlice, date, tokens[x]))
        thread.start()

    # endTime
    end = datetime.now()
    # workTime
    workTimeInSec = (end-start).total_seconds()
    # sleep timeinterval - workTime
    if workTimeInSec < Utils.planTimeInterval:
        time.sleep(Utils.planTimeInterval - workTimeInSec)