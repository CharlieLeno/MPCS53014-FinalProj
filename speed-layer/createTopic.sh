kafka-topics.sh --create --replication-factor 3 --partitions 1 --topic price_qileichen --bootstrap-server $KAFKABROKERS

To confirm the topic, run the following code: 
kafka-topics.sh --list --bootstrap-server $KAFKABROKERS

Send some messages
kafka-console-producer.sh --broker-list $KAFKABROKERS --topic price_qileichen

Consume the messages
kafka-console-consumer.sh --bootstrap-server $KAFKABROKERS --topic price_qileichen --from-beginning
