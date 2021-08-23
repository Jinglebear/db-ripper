from utility import Utils

try:
    from kafka import KafkaProducer    
    import requests
    from datetime import datetime
    import json
except Exception as e:
    Utils.print_error("KafkaProducerParking", "Error while import - " + e)

# callback of kafka if send successfull
def send_on_success(record_metadata):
    Utils.print_log("KafkaProducerParking", 'topic: '+ record_metadata.topic + ' partition: ' + record_metadata.partition)

def process_parking_ids(request_string, header):
    # create producer
    producer = KafkaProducer(bootstrap_servers=Utils.bootstrap_servers)
    try:
        # api request
        response = requests.get(request_string, headers=header)
        if(response.status_code == 200):
            producer.send(topic=Utils.topic_parking,
                          value=response.content).add_callback(send_on_success)
            print(json.dumps(response.content,indent=10))
        else:
            Utils.print_error("KafkaProducerParking", "request fail with code " + response.status_code)
    except Exception as e:
        Utils.print_error("KafkaProducerParking", "request fail - " + e)
    # wait until every producer send his data
    producer.flush()


# ============================
# Main
try:
    # function call
    process_parking_ids(request_string=Utils.get_parking_url(),
                        header=Utils.ParkingHeader)
except Exception as e:
    Utils.print_error("KafkaProducerParking", e)
