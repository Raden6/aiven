#!/usr/bin/env python2
# -*- coding: utf-8 -*-


from kafka import KafkaConsumer
import json
import psycopg2



consumer = KafkaConsumer('IoT-topic',
                         #bootstrap_servers="kafka-nader-nader-10dd.aivencloud.com:20368",
                         bootstrap_servers=['localhost:9092'], 
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                         consumer_timeout_ms=5000
                         #security_protocol="SSL", 
                         #ssl_cafile="/home/nader/aiven_test/ca.pem", 
                         #ssl_certfile="/home/nader/aiven_test/service.cert", 
                         #ssl_keyfile="/home/nader/aiven_test/service.key
                         )


create_table_command = "CREATE TABLE temperature (id SERIAL PRIMARY KEY, temperature_value NUMERIC )"
check_if_table_exists_command = "select exists(select * from information_schema.tables where table_name='temperature');"


uri = "postgres://avnadmin:ff2moukghwhcbzdz@pg-626b02-nader-10dd.aivencloud.com:20366/IoTdb?sslmode=require"

try: 
    print "Connecting to the PostgreSQL database..."
    conn = psycopg2.connect(uri)
    cur = conn.cursor()
    
    cur.execute(check_if_table_exists_command)
    
    if not cur.fetchone()[0] :
        cur.execute(create_table_command)
    

    for message in consumer:
        print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                              message.offset, message.key,
                                              message.value))
        test=message.value
        cur.execute("INSERT INTO temperature(temperature_value) VALUES (%.4f);" % message.value["temperature"])
            
    consumer.close()
    cur.execute("select * from temperature;")
    table = cur.fetchall()
    print "\ntemperature table"
    print "----------------"
    for row in table:
        print row        
    cur.close()
    conn.commit()
    
    
except(Exception, psycopg2.DatabaseError) as error:
    print "Unable to connect to the database"
    print(error)

finally: 
    if conn is not None:
        conn.close();
        print "Database connection closed"    
    
