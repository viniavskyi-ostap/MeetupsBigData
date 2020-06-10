cd /home/andriiprysiazhnyk/Downloads/kafka_2.12-2.4.1/  &&
bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic meetups
bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic US-meetups
bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic US-cities-every-minute
bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic Programming-meetups
bin/kafka-topics.sh --delete --bootstrap-server localhost:9092 --topic __consumer_offsets
