# aiven

Scenario: 

 - IoT temperature sensor, as a Kafka producer, sending temperature values to a Kafka consumer.
 - Kafka consumer receives temperature data from producer and stores it in a PostgeSQL database.
 - Producer and consumer are running on a local Kafka server (using linux machine). => I had problems with the Aiven kafka server
 so I decided to run it locally ..
 - Consumer stores the data received on an Aiven PostgreSQL database called "IoTdb".
