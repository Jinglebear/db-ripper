from datetime import datetime
import time
from kafka import KafkaProducer
import requests 
import xml.etree.ElementTree as ET 

#constants
headers = {
    'Accept': 'application/xml',
    'Authorization': 'Bearer 13d747ec4d3615f93cca7dcf7f203389',
}
timeIntervalInSec = 10
topic = 'RecentChangedTrain'
bootstrap_servers=['localhost:29092']

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

def send_on_success(record_metadata):
    print('topic:',record_metadata.topic,'partition:',record_metadata.partition)

while True:
    #startTime
    start = datetime.now()
    
    #Work
    response = requests.get('https://api.deutschebahn.com/timetables/v1/rchg/8000240', headers=headers)

    producer.send(topic=topic, value=response.content).add_callback(send_on_success)
    producer.flush()

    #endTime
    end = datetime.now()
    #workTime
    workTimeInSec = (end-start).total_seconds()
    #sleep timeinterval-workTime
    if workTimeInSec<timeIntervalInSec:
        time.sleep(timeIntervalInSec-workTimeInSec)

