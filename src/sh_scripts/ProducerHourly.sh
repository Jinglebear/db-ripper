#!/bin/bash

user=bigdata
path=/home/"$user"/db-ripper

python3 "$path"/src/KafkaProducerPlanned.py >> "$path"/misc/logs/logKafkaProducerPlanned.log 2>> "$path"/misc/err_logs/ErrLogKafkaProducerPlanned.log&
python3 "$path"/src/KafkaProducerParking.py >> "$path"/misc/logs/KafkaProducerParking.log 2>>"$path"/misc/err_logs/ErrLogProducerParking.log&
python3 $path/src/KafkaProducerWeather.py >> $path/misc/logs/logKafkaProducerWeather.log 2>> $path/misc/err_logs/ErrLogKafkaProducerWeather.log&