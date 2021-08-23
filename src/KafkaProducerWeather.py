from utility import Utils

try:
    import threading
    import time
    from kafka import KafkaProducer
    import requests
except Exception as e:
    Utils.print_error("KafkaProducerWeather", "Error while import", e)

# iterate over eva numbers and send response to kafka in a thread
def work_thread(city_names, security_token):
    producer = KafkaProducer(bootstrap_servers=Utils.bootstrap_servers)
    
    calls_in_minute=0
    with requests.Session() as session:
        for city in city_names:
            if calls_in_minute < Utils.weather_invocation_limit:
                calls_in_minute += 1
            else:
                time.sleep(60)
                calls_in_minute = 1 
            try:  
                response = session.get(Utils.get_weather_url(city,security_token))

                if response.status_code==200:
                    producer.send(topic=topic, value=response.content)
                else:
                    Utils.print_error("KafkaProducerWeather", "request fail with code " + str(response.status_code) + ", city: " + city)
            except Exception as e:
                Utils.print_error("KafkaProducerWeather", "request fail", e)
        producer.flush()

#=======================================
# Main
try:
    Utils.print_log("KafkaProducerWeather", "start work")
    ##Work
    # load cityNames
    topic = Utils.topic_weather
    city_names = Utils.get_city_name_weather()
    # load tokens
    tokens = Utils.tokens_weather
    # eva numbers that one token will process
    city_per_token = int(len(city_names) / len(tokens)) + 1
    # divide work on token
    for x in range(len(tokens)):
        thread = threading.Thread(target=work_thread, args=(city_names[x*city_per_token:(x+1)*city_per_token], tokens[x],))
        thread.start()
except Exception as e:
    Utils.print_error("KafkaProducerWeather", "Error while main", e)