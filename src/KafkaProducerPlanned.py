from datetime import datetime
import threading
import time
from kafka import KafkaProducer
import requests 
from Utility import Utils
# callback of kafka if send successfull
def send_on_success(record_metadata):
    print('topic:',record_metadata.topic,'partition:',record_metadata.partition)


# process_evas get an slice with eva_numbers, hourSlice and date for the request.
# request api with given eva numbers and save data in kafka
def process_evas(evas, hourSlice, date, security_token):
    # create producer
    producer = KafkaProducer(bootstrap_servers=Utils.bootstrap_servers)

    calls_in_minute = 0
    # iterate over given eva numbers
    with requests.Session() as session:
        for eva in evas:
            # rest if the invocation limit is reached 
            if calls_in_minute < Utils.timetableInvocationLimit:
                calls_in_minute += 1
            else:
                time.sleep(60 - datetime.now().second)
                calls_in_minute = 0
            
            # api request
            header = TimeTableHeader1 = headers = {
                'Accept': 'application/xml',
                'Authorization': security_token,
            }   
            try:
                response = session.get(Utils.get_planned_url(eva,date,str(hourSlice)), headers=header)
                # if api request was successfull send data to kafka
                if response.status_code == 200:
                    producer.send(topic=Utils.topicForPlannedTimetables, value=response.content).add_callback(send_on_success)
            except Exception as e:
                print(e)
    # wait until every producer send his data
    producer.flush()


#=======================================
# Main 
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
    tokens = Utils.tokenListTimeTablePlanned
    # eva numbers that one token will process
    evas_per_token = int(len(evas) / len(tokens)) + 1
    # divide work on token
    for x in range(len(tokens)):
        thread = threading.Thread(target=process_evas, args=(evas[x*evas_per_token:(x+1)*evas_per_token], hourSlice, date, tokens[x]))
        thread.start()

    # endTime
    end = datetime.now()
    # workTime
    workTimeInSec = (end-start).total_seconds()
    # sleep timeinterval - workTime
    if workTimeInSec < Utils.planTimeInterval:
        time.sleep(Utils.planTimeInterval - workTimeInSec)