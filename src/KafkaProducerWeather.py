from datetime import datetime
import threading
import time
from kafka import KafkaProducer
import requests
from utility import Utils

# load constants
authTokenList=Utils.tokens_weather

timeIntervalInSec = Utils.weatherTimeInterval
topic = Utils.topic_weather



# iterate over eva numbers and send response to kafka in a thread
def work_thread(city_names, security_token):
    producer = KafkaProducer(bootstrap_servers=Utils.bootstrap_servers)
    
    
    calls_in_minute=0
    with requests.Session() as session:
        for city in city_names:
            if calls_in_minute < Utils.weather_invocation_limit:
                calls_in_minute += 1
            else:
                time.sleep(60 - datetime.now().second)
                calls_in_minute = 0 
            try:  
                response = session.get(Utils.get_weather_url(city,security_token))
                if response.status_code==200:
                    producer.send(topic=topic, value=response.content)
            except Exception as e:
                print(e)
        producer.flush()


# Produce information end send to kafka
##Work
# load cityNames
cityNames = Utils.get_city_name_weather()
# load tokens
tokens = Utils.tokens_weather
# eva numbers that one token will process
city_per_token = int(len(cityNames) / len(tokens)) + 1
# divide work on token
for x in range(len(tokens)):
    thread = threading.Thread(target=work_thread, args=(cityNames[x*city_per_token:(x+1)*city_per_token], tokens[x]))
    thread.start()