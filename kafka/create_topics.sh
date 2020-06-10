cd /home/andriiprysiazhnyk/Downloads/kafka_2.12-2.4.1/  &&
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic meetups --config retention.ms=25200000
