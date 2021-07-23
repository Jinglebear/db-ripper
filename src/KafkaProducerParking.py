import sys
from datetime import datetime
try:
    import time
    from kafka import KafkaProducer, producer
    from utility import Utils
    import requests
except Exception as e:
    print("#",datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaProducerParking: Exception by import",e,file=sys.stderr)
# callback of kafka if send successfull
def send_on_success(record_metadata):
    print('topic:',record_metadata.topic,'partition:',record_metadata.partition)


def process_parking_IDs(request_string, header):
    #create producer
    producer=KafkaProducer(bootstrap_servers=Utils.bootstrap_servers)
    try:
        #api request
        response = requests.get(request_string,headers=header)
        if(response.status_code == 200):
            producer.send(topic=Utils.topicParkingTimetables,value=response.content).add_callback(send_on_success)
        else:
            print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaProducerParking: request fail with code", response.status_code, file=sys.stderr)
    except Exception as e:
        print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaProducerParking: request fail", e, file=sys.stderr)
    # wait until every producer send his data
    producer.flush()

#============================
#Main
try:
#constants
    request_string = "https://api.deutschebahn.com/bahnpark/v1/spaces/occupancies" # /spaces/{id}/occupancies
    header = {'Accept': 'application/json;charset=utf-8','Authorization': 'Bearer eed6fa376f86dce95beb400315616e58'}
    #function call
    process_parking_IDs(request_string=request_string,header=header)
except Exception as e:
    print("#", datetime.now().strftime("%Y-%m-%d %H:%M:%S"),"KafkaProducerParking: Exception in main", e, file=sys.stderr)


