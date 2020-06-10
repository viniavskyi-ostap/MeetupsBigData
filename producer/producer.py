import json
import requests
import kafka

BOOTSTRAP_SERVERS = ["172.31.42.74:9092", "172.31.45.18:9092", "172.31.43.240:9092"]
URL = "http://stream.meetup.com/2/rsvps"
TOPIC_NAME = "meetups"

if __name__ == '__main__':

    producer = kafka.KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS,
                                   value_serializer=lambda x:
                                   json.dumps(x).encode("utf-8"))

    response = requests.get(URL, stream=True)

    try:
        for line in response.iter_lines(decode_unicode=True):
            if line:
                producer.send(value=json.loads(line), topic=TOPIC_NAME)
                print("Sent to Kafka")
    finally:
        producer.flush()
        producer.close()
