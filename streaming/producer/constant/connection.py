
import os

os.environ["KAFKA_BROKER"] = os.environ.get("KAFKA_BROKER") if os.environ.get("KAFKA_BROKER") else "localhost:9092"
os.environ["KAFKA_TOPIC"] = os.environ.get("KAFKA_TOPIC") if os.environ.get("KAFKA_TOPIC") else "data"

KAFKA_BROKER = os.environ.get("KAFKA_BROKER").split(',')
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC").split(',')