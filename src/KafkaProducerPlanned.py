from datetime import datetime
import threading
import time
from kafka import KafkaProducer
import requests 
import Utils


# iterate over eva numbers and send response to kafka in a thread
def work_thread(producer, hourSlice, date, eva_numbers):
    for eva in eva_numbers:
        response = requests.get(Utils.get_planned_url(eva,date,str(hourSlice)), headers=Utils.TimeTableHeader1)
    
        producer.send(topic=topic, value=response.content).add_callback(send_on_success)

    producer.flush()


# load constants
headers = Utils.TimeTableHeader1
timeIntervalInSec = Utils.planTimeInterval
topic = Utils.topicForPlannedTimetables
bootstrap_servers=Utils.bootstrap_servers

producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

def send_on_success(record_metadata):
    print('topic:',record_metadata.topic,'partition:',record_metadata.partition)

# get eva-number from csv
csvfile = Utils.cityEvaRead()
eva_numbers = []
for line in csvfile:
    lineArr = line.strip().split(",")
    eva_numbers.append(lineArr[1])

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

    # work in a thread
    thread = threading.Thread(target=work_thread, args=(producer, hourSlice, date, eva_numbers))
    thread.start()

    # endTime
    end = datetime.now()
    # workTime
    workTimeInSec = (end-start).total_seconds()
    # leep timeinterval-workTime
    if workTimeInSec<timeIntervalInSec:
        time.sleep(timeIntervalInSec-workTimeInSec)