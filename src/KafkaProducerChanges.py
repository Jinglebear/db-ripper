import sys
from datetime import datetime

try:
    import threading
    import time
    from kafka import KafkaProducer
    import requests 
    from utility import Utils
except Exception as e:
    print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaProducerChanges: Exception by import", e, file=sys.stderr)


# iterate over eva numbers and send response to kafka in a thread
def work_thread(eva_numbers, security_token):

    producer = KafkaProducer(bootstrap_servers=Utils.bootstrap_servers)
    
    calls_in_minute=0
    with requests.Session() as session:
        for eva in eva_numbers:
            header = {
                'Accept': 'application/xml',
                'Authorization': security_token,
            } 
            try:  
                response = session.get(Utils.get_changes_url(eva), headers=header)
                if response.status_code==200:
                    producer.send(topic=Utils.topicForChangedTimetabled, value=response.content)
                else:
                    print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaProducerChanges: request fail with code", response.status_code, file=sys.stderr)
            except Exception as e:
                print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaProducerChanges: request fail", e, file=sys.stderr)
        producer.flush()

## Work
try:
    print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaProducerChanges: start work")
    # Produce information end send to kafka
    # preparatory work: set hourslice and date
    start = datetime.now()
    hourSlice = start.hour
    # date in format: YYMMDD
    date = (str(start.year%1000) + 
        (('0'+str(start.month)) if (start.month<10) else (str(start.month))) + 
        (('0'+str(start.day)) if (start.day<10) else (str(start.day))))

    ##Work
    # load eva numbers
    evas = Utils.get_eva_numbers()
    # load tokens
    tokens = Utils.tokenlistTimetable
    # eva numbers that one token will process
    evas_per_token = int(len(evas) / len(tokens)) + 1
    # divide work on token
    for x in range(len(tokens)):
        thread = threading.Thread(target=work_thread, args=(evas[x*evas_per_token:(x+1)*evas_per_token], tokens[x]))
        thread.start()
except Exception as e:
    print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaProducerChanges: Exception in main", e, file=sys.stderr)