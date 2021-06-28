from datetime import datetime
import time
from kafka import KafkaProducer
import requests 
import Utils as u

# load constants
headers = u.TimeTableHeader1
timeIntervalInSec = u.planTimeInterval
topic = u.topicForPlannedTimetables
bootstrap_servers=u.bootstrap_servers

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

def send_on_success(record_metadata):
    print('topic:',record_metadata.topic,'partition:',record_metadata.partition)

# get eva-number from csv
csvfile = u.cityEvaRead
eva_numbers = []
for line in csvfile:
    lineArr = line.strip().split(",")
    eva_numbers.append(lineArr[1])

# begin to work when on the clock the minute is 00
now = datetime.now()
time.sleep(60-now.seconds)

# Produce information end send to kafka
while True:
    ##startTime
    start = datetime.now()

    ##Work
    hourSlice = start.hour
    # date in format: YYMMDD
    date = (str(start.year%1000) + 
        (('0'+str(start.month)) if (start.month<10) else (str(start.month))) + 
        (('0'+str(start.day)) if (start.day<10) else (str(start.day))))

    # iterate over cities
    for eva in eva_numbers:
        response = requests.get(u.get_planned_url(eva,date,str(hourSlice)))
    
        producer.send(topic=topic, value=response.content).add_callback(send_on_success)

    producer.flush()

    #endTime
    end = datetime.now()
    #workTime
    workTimeInSec = (end-start).total_seconds()
    #sleep timeinterval-workTime
    if workTimeInSec<timeIntervalInSec:
        time.sleep(timeIntervalInSec-workTimeInSec)

