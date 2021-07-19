from kafka import KafkaConsumer

consumer = KafkaConsumer('changed', group_id='my_group', bootstrap_servers=['localhost:29092'])

for message in consumer:
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition, message.offset, message.key, message.value))