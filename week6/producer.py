from confluent_kafka import Producer
from config import read_ccloud_config

if __name__ == "__main__":
  producer = Producer(read_ccloud_config("config.txt"))
  producer.produce("my-topic", key="key", value="value")