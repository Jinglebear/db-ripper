#!/bin/bash

printf 'Starting KafkaConsumerPlanned..\n'

path=/home/"$USER"/db-ripper

python3 $path/src/KafkaConsumerPlanned.py >> $path/misc/logs/logKafkaConsumerPlanned.log 2>> $path/misc/err_logs/ErrLogKafkaConsumerPlanned.log