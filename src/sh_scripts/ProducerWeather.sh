#!/bin/bash

printf 'Starting KafkaProducerWeather..\n'

path=/home/"$USER"/db-ripper

python3 $path/src/KafkaProducerWeather.py >> $path/misc/logs/logKafkaProducerWeather.log 2>> $path/misc/err_logs/ErrLogKafkaProducerWeather.log