import sys
from datetime import datetime, timedelta

import kafka

try:
    import threading
    from kafka import KafkaConsumer
    import xml.etree.ElementTree as ET
    import json
    from utility import Utils
except Exception as e:
    print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaConsumerWeather: Exception by import", e, file=sys.stderr)

# save incoming json on elasticsearch
def save_on_elasticsearch(weatherJson, id):
    # connect to elasticsearch with default config
    _es = Utils.connect_elasticsearch()
    if _es == None:
        print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaConsumerWeather: Can't connect to elasticsearch", file=sys.stderr)
        return

    # create index if not exists with defaultname
    Utils.create_index(_es)

    # store json on elasticsearch
    try:
        response = _es.index(Utils.esIndex, body=weatherJson)
    except Exception as e:
        print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaConsumerWeather: Error while indexing data.", e, file=sys.stderr)
        


# factorize incoming message in a thread
# create json and give them to save_on_elasticsearch
def factorize_message(kafkaJsonWeatherData):

    try:
        # save values in weatherInformation
        weatherInformation = {}

        weatherInformation['cityname']= kafkaJsonWeatherData['name']
        coord= [kafkaJsonWeatherData['coord']['lon'],kafkaJsonWeatherData['coord']['lat']]
        weatherInformation['Coordinaten']=coord

        weatherInformation['weatherId']=kafkaJsonWeatherData['weather']['id']
        weatherInformation['weatherDiscription']= kafkaJsonWeatherData['weather']['main']
        weatherInformation['weatherSpecificDiscription']=kafkaJsonWeatherData['weather']['description']

        weatherInformation['temprature']=kafkaJsonWeatherData['main']['temp']
        weatherInformation['temprature_Min']=kafkaJsonWeatherData['main']['temp_min']
        weatherInformation['temprature_Max']=kafkaJsonWeatherData['main']['temp_max']

        weatherInformation['wind_Speed']= kafkaJsonWeatherData['wind']['speed']

        currentDT = datetime.now() + timedelta(hours=1)
        currentDT_formated = currentDT.strftime("%Y-%m-%dT%H:%M:%S")
        #add formatted timestamp to weatherInformation JSON Object
        weatherInformation['timestamp'] = currentDT_formated

        # transform dictinary to json and save on elasticsearch
        json_object = json.dumps(weatherInformation)
        save_on_elasticsearch(json_object, weatherInformation['id'])# nach Wetterid ablegen oder CityName
    except Exception as e:
        print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaConsumerWeather: Error in an event", file=sys.stderr)

## Work
print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "KafkaConsumerWeather: start consumer")
consumer = KafkaConsumer(Utils.topicWeather, group_id='db_ripper' , bootstrap_servers=Utils.bootstrap_servers)

for message in consumer:
    messageValue = message.value
    messageValueAsString = messageValue.decode('utf-8')
    thread = threading.Thread(target=factorize_message, args=(json.loads(messageValueAsString),))
    thread.start()

for message in consumer:
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))