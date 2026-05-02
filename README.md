1- kafka-storage.bat random-uuid
2- kafka-storage.bat format -t oRk-t8DATGODmIJqufgg_w -c ..\..\config\server.properties
3- kafka-server-start.bat ..\..\config\server.properties
4- kafka-topics.bat --create --topic gi-topic2 --bootstrap-server localhost:9092

// for console application
kafka-console-producer.bat --topic gi-topic2 --bootstrap-server localhost:9092
kafka-console-consumer.bat --topic gi-topic2 --from-beginning --bootstrap-server localhost:9092
