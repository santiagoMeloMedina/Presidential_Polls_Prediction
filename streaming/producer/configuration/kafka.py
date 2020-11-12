
from utility.kafka import KafkaClient
import constant.connection as CONN

producer = KafkaClient(**{
    "producer": True, 
    "brokers": CONN.KAFKA_BROKER, 
    "topics": CONN.KAFKA_TOPIC
    })