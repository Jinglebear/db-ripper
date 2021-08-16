from datetime import datetime
import threading
import time
from kafka import KafkaProducer
import requests
from utility import Utils

# load constants
authTokenList=Utils.tokenlistWeather

timeIntervalInSec = Utils.weatherTimeInterval
topic = Utils.topicWeather



# iterate over eva numbers and send response to kafka in a thread
def work_thread(cityNames, security_token):
    producer = KafkaProducer(bootstrap_servers=Utils.bootstrap_servers)
    
    
    print(producer)
    
    calls_in_minute=0
    with requests.Session() as session:
        for city in cityNames:
            if calls_in_minute < Utils.weatherInvocationLimit:
                calls_in_minute += 1
            else:
                time.sleep(60 - datetime.now().second)
                calls_in_minute = 0 
            try:  
                response = session.get(Utils.get_weather_url(city,security_token))
                print(response.text)
                if response.status_code==200:
                    producer.send(topic=topic, value=response.content)
            except Exception as e:
                print(e)
        producer.flush()


# Produce information end send to kafka
##Work
# load cityNames
cityNames = Utils.get_cityName_Weather()
print(cityNames ,"\n\n\n")
# load tokens
tokens = Utils.tokenlistWeather
print("Tokens: \n" , tokens)
# eva numbers that one token will process
# city_per_token = int(len(cityNames) / len(tokens)) + 1
# divide work on token
# for x in range(len(tokens)):
#     thread = threading.Thread(target=work_thread, args=(cityNames[x*city_per_token:(x+1)*city_per_token], tokens[x]))
#     thread.start()
print("Method call: \n\n\n")
work_thread(cityNames=cityNames,security_token=tokens[0])