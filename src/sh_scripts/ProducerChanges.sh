#!/bin/bash

printf 'Starting KafkaProducerChanges..\n'

path=/home/"$USER"/db-ripper

python3 $path/src/KafkaProducerChanges.py >> $path/misc/logs/logKafkaProducerChanges.log 2>> $path/misc/err_logs/ErrLogKafkaProducerChanges.log