#!/bin/bash
#Log message
printf 'Starting KafkaProducerParking...\n'

python3 /home/"$USER"/db-ripper/src/KafkaProducerParking.py >> /home/"$USER"/db-ripper/misc/logs/KafkaProducerParking.log 2>>/home/"$USER"/db-ripper/misc/err_logs/ErrLogProducerParking.log