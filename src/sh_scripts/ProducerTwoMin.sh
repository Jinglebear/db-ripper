#!/bin/bash

user=bigdata
path=/home/"$user"/db-ripper

python3 "$path"/src/KafkaProducerChanges.py >> "$path"/misc/logs/logKafkaProducerChanges.log 2>> "$path"/misc/err_logs/ErrLogKafkaProducerChanges.log&