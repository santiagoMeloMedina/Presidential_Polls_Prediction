
from confluent_kafka import Producer, Consumer
import constant.kafka as KAFKA
from threading import Thread

class KafkaClient:
    def __init__(self, producer=False, consumer=False, groupId=None, brokers=[], topics=[]):
        if (producer and consumer) or (not producer and not consumer): raise Exception(KAFKA.ERROR_BOTH_CLIENT)
        self.type = KAFKA.CONSUMER_TYPE if consumer else KAFKA.PRODUCER_TYPE
        self.specs = {"groupId": groupId, "brokers": brokers, "topics": topics}
        if self.type == KAFKA.CONSUMER_TYPE:
            self.__setConsumer()
        else:
            self.__setProducer()
    
    def __setConsumer(self):
        """This function is used by consumer type client as a default, 
            and is responsible for setting the consumer client and 
            subscribing to a topic specified"""
        self.client = Consumer({
            'bootstrap.servers': ','.join(self.specs["brokers"]), 
            'group.id': self.specs['groupId'], 
            'auto.offset.reset': 'earliest'
            })
        self.client.subscribe(self.specs["topics"])
        return self
    
    def __setProducer(self):
        """This function is used by producer type client as a default, 
            and is responsible to set the producer client"""
        self.client = Producer({
            'bootstrap.servers': ','.join(self.specs["brokers"])
            })
        return self
    
    def __useMessage(self, message, actionable, action):
        """This function triggers a actions passed by parameter if 
            another verification function passed by parameter meet the 
            its conditions, these functions take the message consumed"""
        if actionable(message):
            action(message)
        return
    
    def __consume(self, actionable, action):
        """This function starts consuming messages fromm the specified broker 
            and acts on them with the specified functions"""
        stop = False
        # self.threads = []
        while not stop:
            message = self.client.poll(KAFKA.CONSUMER_POLL_TIME)
            if message != None:
                self.__useMessage(message.value().decode("utf-8"), actionable, action)
        self.client.close()
        return
    
    def __produce(self, message):
        """This function produces a message the specified broker"""
        self.client.produce(self.specs["topics"][0], message)
        self.client.flush()
        return
    
    def perform(self, message=None, actionable=None, action=None):
        """This function perform the action of consuming or producing 
            depending of the type of the client"""
        if self.type == KAFKA.PRODUCER_TYPE:
            if message != None:
                self.__produce(message)
            else:
                print(KAFKA.ERROR_PERFORM_MISSING_PARAMS)
        else:
            if actionable != None and action != None:
                self.__consume(actionable, action)
            else:
                print(KAFKA.ERROR_PERFORM_MISSING_PARAMS)
        return self

