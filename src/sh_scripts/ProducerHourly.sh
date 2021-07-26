#!/bin/bash

user=johannes
path=/home/"$user"/db-ripper

python3 "$path"/src/KafkaProducerPlanned.py >> "$path"/misc/logs/logKafkaProducerPlanned.log 2>> "$path"/misc/err_logs/ErrLogKafkaProducerPlanned.log&