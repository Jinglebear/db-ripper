from utility import Utils
import sys
from datetime import datetime

try:
    import threading
    import time
    from kafka import KafkaProducer
    import requests 
    import sys
    from datetime import datetime
except Exception as e:
    Utils.print_error("KafkaProducerPlanned", "Error while import", e)

# process_evas get an slice with eva_numbers, hourSlice and date for the request.
# request api with given eva numbers and save data in kafka
def process_evas(evas, hour_slice, date, security_token):
    # create producer
    producer = KafkaProducer(bootstrap_servers=Utils.bootstrap_servers)

    calls_in_minute = 0
    # iterate over given eva numbers
    with requests.Session() as session:
        for eva in evas:
            # rest if the invocation limit is reached 
            if calls_in_minute < Utils.timetable_invocation_limit:
                calls_in_minute += 1
            else:
                time.sleep(60)
                calls_in_minute = 1
            
            # api request
            header = {
                'Accept': 'application/xml',
                'Authorization': security_token,
            }   
            try:
                response = session.get(Utils.get_planned_url(eva,date,str(hour_slice)), headers=header)
                # if api request was successfull send data to kafka
                if response.status_code == 200:
                    producer.send(topic=Utils.topic_timetable_planned, value=response.content)
                else:
                    Utils.print_error("KafkaProducerPlanned", "request fail with code " + str(response.status_code))
            except Exception as e:
                Utils.print_error("KafkaProducerPlanned", "request fail", e)
    # wait until every producer send his data
    producer.flush()


#=======================================
# Main
try:
    Utils.print_log("KafkaProducerPlanned", "start work")
    # preparatory work: set hourslice and date
    start = datetime.now()
    hour_slice = start.hour
    # date in format: YYMMDD
    date = (str(start.year%1000) + 
        (('0'+str(start.month)) if (start.month<10) else (str(start.month))) + 
        (('0'+str(start.day)) if (start.day<10) else (str(start.day))))

    ##Work
    # load eva numbers
    evas = Utils.get_eva_numbers()
    # load tokens
    tokens = Utils.tokens_table_planned
    # eva numbers that one token will process
    evas_per_token = int(len(evas) / len(tokens)) + 1
    # divide work on token
    for x in range(len(tokens)):
        thread = threading.Thread(target=process_evas, args=(evas[x*evas_per_token:(x+1)*evas_per_token], hour_slice, date, tokens[x]))
        thread.start()
except Exception as e:
    Utils.print_error("KafkaProducerPlanned", "Error while main", e)