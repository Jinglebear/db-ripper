#!/bin/bash
#Log message
printf 'Starting KafkaConsumerParking...\n'

python3 /home/"$USER"/db-ripper/src/KafkaConsumerParking.py >> /home/"$USER"/db-ripper/misc/logs/KafkaConsumerParking.log 2>>/home/"$USER"/db-ripper/misc/err_logs/ErrLogConsumerParking.log 