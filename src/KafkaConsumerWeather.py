from utility import Utils

try:
    from kafka import KafkaConsumer
    from datetime import datetime
    from kafka import consumer
    import json
except Exception as e:
    Utils.print_error("KafkaConsumerWeather", "Error while import", e)

# save incoming json on elasticsearch
def save_on_elasticsearch(weather_json):
    # connect to elasticsearch with default config
    _es = Utils.connect_elasticsearch()
    if _es == None:
        Utils.print_error("KafkaConsumerWeather", "Can't connect to elasticsearch")
        return

    # create index if not exists with defaultname
    Utils.create_index(_es)

    # store json on elasticsearch
    try:
        _es.index(Utils.es_default_index, body=weather_json)
        # print(response)
    except Exception as e:
        Utils.print_error("KafkaConsumerWeather", "Error while indexing data", e)
        


# factorize incoming message in a thread
# create json and give them to save_on_elasticsearch
def factorize_message(kafka_json_weather_data): 
    try:
        # save values in weatherInformation
        weather_information = {}

        weather_information['city']= kafka_json_weather_data['name']

        coord= [kafka_json_weather_data['coord']['lon'], kafka_json_weather_data['coord']['lat']]

        weather_information['location']=coord
        
        weather_information['weatherDescription']= kafka_json_weather_data['weather'][0]['main']

        weather_information['weatherSpecificDescription'] = kafka_json_weather_data['weather'][0]['description']

        temp_in_kelvin = kafka_json_weather_data['main']['temp']
        weather_information['temperature'] = round(temp_in_kelvin - 273.15, 2)

        weather_information['windSpeed']= kafka_json_weather_data['wind']['speed']

        weather_information['event']='weather'

        #add formatted timestamp to weatherInformation JSON Object
        current_dt = datetime.now()
        weather_information['timestamp'] = current_dt.strftime(Utils.timestamp_format)

        # transform dictinary to json and save on elasticsearch
        json_object = json.dumps(weather_information,indent=4)

        save_on_elasticsearch(json_object)
    except Exception as e:
        Utils.print_error("KafkaConsumerWeather", "Error while processing json from kafka", e)

## Work
Utils.print_log("KafkaConsumerWeather", "start consumer")
consumer=KafkaConsumer(Utils.topic_weather, group_id='db-ripper', bootstrap_servers=Utils.bootstrap_servers)
print(consumer.bootstrap_connected())
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    messageValue = message.value
    messageValueAsString = messageValue.decode('utf-8')
    factorize_message(json.loads(messageValueAsString))