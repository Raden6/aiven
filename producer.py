from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging as log
from time import sleep
import json
import random



producer = KafkaProducer(
        #bootstrap_servers="kafka-nader-nader-10dd.aivencloud.com:20368",
        bootstrap_servers='localhost:9092',
        value_serializer=lambda m: json.dumps(m).encode('utf-8'),
        acks='all'
        #security_protocol="SSL", 
        #ssl_cafile="/home/nader/aiven_test/ca.pem", 
        #ssl_certfile="/home/nader/aiven_test/service.cert", 
        #ssl_keyfile="/home/nader/aiven_test/service.key"
        )


def on_send_error(exception):
    log.error('Error: Could not send message', exception)

for _ in range(10):
#while 1:    
    temperature = round(random.uniform(14.0,16.0), 4)
    json_msg = {'temperature': temperature}
    future = producer.send('IoT-topic',
                  json_msg).add_errback(on_send_error)
    try:
        record_metadata = future.get(timeout=10)
    except KafkaError as error:
        log.exception(error)
        pass
        
    print 'offset: ', record_metadata.offset
    sleep(0.5)
    
producer.close();