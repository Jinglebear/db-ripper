from utility import Utils

try:
    import threading
    from kafka import KafkaProducer
    import requests
    import sys
    from datetime import datetime
except Exception as e:
    Utils.print_error("KafkaPrdoucerChanges", "Error while import - " + e)


# iterate over eva numbers and send response to kafka in a thread
def work_thread(eva_numbers, security_token):

    producer = KafkaProducer(bootstrap_servers=Utils.bootstrap_servers)
    
    with requests.Session() as session:
        for eva in eva_numbers:
            header = {
                'Accept': 'application/xml',
                'Authorization': security_token,
            } 
            try:  
                response = session.get(Utils.get_changes_url(eva), headers=header)
                if response.status_code==200:
                    producer.send(topic=Utils.topic_timetable_changed, value=response.content)
                else:
                    Utils.print_error("KafkaProducerChanges", "Request fail with code " + response.status_code)
            except Exception as e:
                Utils.print_error("KafkaProducerChanges", "request fail - " + e)
        producer.flush()

## Work
try:
    Utils.print_log("KafkaProducerChanges", "start work")
    # Produce information end send to kafka
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
    tokens = Utils.tokens_timetable
    # eva numbers that one token will process
    evas_per_token = int(len(evas) / len(tokens)) + 1
    # divide work on token
    for x in range(len(tokens)):
        thread = threading.Thread(target=work_thread, args=(evas[x*evas_per_token:(x+1)*evas_per_token], tokens[x]))
        thread.start()
except Exception as e:
    Utils.print_error("KafkaProducerChanges", "Error while main - " + e)