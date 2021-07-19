from datetime import datetime
import threading
import time
from kafka import KafkaProducer
import requests
from utility import Utils

# load constants
authTokenList=Utils.tokenlistWaether

timeIntervalInSec = Utils.weatherTimeInterval
topic = Utils.topicWeather

def send_on_success(record_metadata):
    print('topic:',record_metadata.topic,'partition:',record_metadata.partition)


# iterate over eva numbers and send response to kafka in a thread
def work_thread(cityNames, security_token):

    producer = KafkaProducer(bootstrap_servers=Utils.bootstrap_servers)
    
    
    calls_in_minute=0
    with requests.Session() as session:
        for city in cityNames:
            if calls_in_minute < Utils.weatherInvocationLimit:
                calls_in_minute += 1
            else:
                time.sleep(60 - datetime.now().second)
                calls_in_minute = 0 
            try:  
                response = requests.get(Utils.get_weather_url(city,security_token))
                if response.status_code==200:
                    producer.send(topic=topic, value=response.content).add_callback(send_on_success)
            except Exception as e:
                print(e)
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
    # load cityNames
    cityNames = Utils.get_cityName()
    # load tokens
    tokens = Utils.tokenlistWaether
    # eva numbers that one token will process
    city_per_token = int(len(cityNames) / len(tokens)) + 1
    # divide work on token
    for x in range(len(tokens)):
        thread = threading.Thread(target=work_thread, args=(cityNames[x*city_per_token:(x+1)*city_per_token], tokens[x]))
        thread.start()

    # endTime
    end = datetime.now()
    # workTime
    workTimeInSec = (end-start).total_seconds()
    # sleep timeinterval - workTime
    if workTimeInSec < Utils.planTimeInterval:
        time.sleep(Utils.planTimeInterval - workTimeInSec)