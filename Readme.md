Start kafka locally

bin/zookeeper-server-start.sh config/zookeeper.properties

bin/kafka-server-start.sh config/server.properties

bin/kafka-console-producer.sh --broker-list localhost:9092 --topic final

bin/connect-standalone.sh config/connect-standalone.properties config/connect-directory-source.properties

bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic final --from-beginning