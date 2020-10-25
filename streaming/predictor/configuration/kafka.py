
from utility.kafka import KafkaClient
import constant.connection as CONN

consumer = KafkaClient(**{
    "consumer": True, 
    "brokers": CONN.KAFKA_BROKER, 
    "topics": CONN.KAFKA_TOPIC, 
    "groupId": CONN.KAFKA_DEFAULT_GROUP
    })