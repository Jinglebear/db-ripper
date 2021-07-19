#!/bin/bash
#Log message
printf 'Starting KafkaConsumerParking...\n'

python3 /home/nils/db-ripper/src/KafkaConsumerParking.py >> /home/nils/db-ripper/misc/logs/KafkaConsumerParking.log 2>>/home/nils/db-ripper/misc/err_logs/ErrLogConsumerParking.log 