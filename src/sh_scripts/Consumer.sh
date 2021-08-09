#!/bin/bash

user=bigdata
path=/home/"$user"/db-ripper

python3 "$path"/src/KafkaConsumerPlanned.py >> "$path"/misc/logs/logKafkaConsumerPlanned.log 2>> "$path"/misc/err_logs/ErrLogKafkaConsumerPlanned.log&
python3 "$path"/src/KafkaConsumerChanges.py >> "$path"/misc/logs/logKafkaConsumerChanges.log 2>> "$path"/misc/err_logs/ErrLogKafkaConsumerChanges.log&
python3 "$path"/src/KafkaConsumerParking.py >> "$path"/misc/logs/logKafkaConsumerParking.log 2>> "$path"/misc/err_logs/ErrLogKafkaConsumerParking.log&
python3 "$path"/src/KafkaConsumerWeather.py >> "$path"/misc/logs/logKafkaConsumerWeather.log 2>> "$path"/misc/err_logs/ErrLogKafkaConsumerWeather.log&