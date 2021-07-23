#!/bin/bash

path=/home/johannes/db-ripper

python3 "$path"/src/KafkaConsumerPlanned.py >> "$path"/misc/logs/logKafkaConsumerPlanned.log 2>> "$path"/misc/err_logs/ErrLogKafkaConsumerPlanned.log&
python3 "$path"/src/KafkaConsumerChanges.py >> "$path"/misc/logs/logKafkaConsumerChanges.log 2>> "$path"/misc/err_logs/ErrLogKafkaConsumerChanges.log&