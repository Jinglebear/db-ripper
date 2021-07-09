from datetime import datetime
import time
from kafka import KafkaProducer, producer
from Utility import Utils
import requests
#constants
request_string = "https://api.deutschebahn.com/bahnpark/v1/spaces/occupancies" # /spaces/{id}/occupancies
header = {
    'Accept': 'application/json;charset=utf-8',
    'Authorization': 'Bearer eed6fa376f86dce95beb400315616e58'
}
# callback of kafka if send successfull
def send_on_success(record_metadata):
    print('topic:',record_metadata.topic,'partition:',record_metadata.partition)


def process_parking_IDs(request_string, header):
    #create producer
    producer=KafkaProducer(bootstrap_servers=Utils.bootstrap_servers)
    try:
        response = requests.get(request_string,headers=header)
        if(response.status_code == 200):
            producer.send(topic=Utils.topicParkingTimetables,value=response.content).add_callback(send_on_success)
    except Exception as e:
        print(e)
    producer.flush()

#============================
#Main

while True:
    process_parking_IDs(request_string=request_string,header=header)
    time.sleep(60)

