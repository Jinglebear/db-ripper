import sys
from datetime import datetime, timedelta
import time
from kafka import consumer



try:
    import threading
    from kafka import KafkaConsumer
    import xml.etree.ElementTree as ET
    import json
    from utility import Utils
except Exception as e:
    print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaConsumerWeather: Exception by import", e)

# save incoming json on elasticsearch
def save_on_elasticsearch(weatherJson):
    print(json.loads(weatherJson))
    # connect to elasticsearch with default config
    _es = Utils.connect_elasticsearch()
    time.sleep(2)
    if _es == None:
        print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaConsumerWeather: Can't connect to elasticsearch")
        return

    # create index if not exists with defaultname
    Utils.create_index(_es)

    # store json on elasticsearch
    try:
        _es.index(Utils.esIndex, body=weatherJson)
        print("Success!")
        # print(response)
    except Exception as e:
        print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaConsumerWeather: Error while indexing data.", e)
        


# factorize incoming message in a thread
# create json and give them to save_on_elasticsearch
def factorize_message(kafkaJsonWeatherData):
    
    try:

        # print(type(kafkaJsonWeatherData))
        
        # save values in weatherInformation
        weatherInformation = {}

        weatherInformation['cityname']= kafkaJsonWeatherData['name']

        coord= [kafkaJsonWeatherData['coord']['lon'],kafkaJsonWeatherData['coord']['lat']]

        weatherInformation['coordinates']=coord
        
        # weatherInformation['weatherDescription']= kafkaJsonWeatherData['weather']['main']

        # weatherInformation['weatherSpecificDiscription']=kafkaJsonWeatherData['weather']['description']

        # weatherInformation['temperature']=kafkaJsonWeatherData['main']['temp']

        # weatherInformation['temperature_Min']=kafkaJsonWeatherData['main']['temp_min']

        # weatherInformation['temperature_Max']=kafkaJsonWeatherData['main']['temp_max']

        # weatherInformation['wind_Speed']= kafkaJsonWeatherData['wind']['speed']

        weatherInformation['event']='weather'

        currentDT = datetime.now()
        currentDT_formated = currentDT.strftime("%Y-%m-%dT%H:%M:%S")
        print(currentDT_formated)
        #add formatted timestamp to weatherInformation JSON Object
        weatherInformation['timestamp'] = currentDT_formated

        # transform dictinary to json and save on elasticsearch
        json_object = json.dumps(weatherInformation,indent=4)
        print(json_object)
        # print(json_object)
        save_on_elasticsearch(json_object)# nach Wetterid ablegen oder CityName
    except Exception as e:
        print(e)
        # print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaConsumerWeather: Error in an event",e, file=sys.stderr)

## Work
print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "KafkaConsumerWeather: start consumer")
# consumer = KafkaConsumer(Utils.topicWeather,group_id='db_ripper', bootstrap_servers=Utils.bootstrap_servers)
consumer=KafkaConsumer('weather',group_id='db-ripper',bootstrap_servers=['localhost:29092'])
print(consumer.bootstrap_connected())
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    # print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
    #                                       message.offset, message.key,
    #                                       message.value))
    messageValue = message.value
    messageValueAsString = messageValue.decode('utf-8')
    factorize_message(json.loads(messageValueAsString))
# print("rndmom")
# for message in consumer:
#     messageValue = message.value
#     messageValueAsString = messageValue.decode('utf-8')
#     # thread = threading.Thread(target=factorize_message, args=(json.loads(messageValueAsString),))
#     # thread.start()
#     factorize_message(messageValueAsString)

# for message in consumer:
#     print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))