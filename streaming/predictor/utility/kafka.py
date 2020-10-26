
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
        self.client = Consumer({
            'bootstrap.servers': ','.join(self.specs["brokers"]), 
            'group.id': self.specs['groupId'], 
            'auto.offset.reset': 'earliest'
            })
        self.client.subscribe(self.specs["topics"])
        return self
    
    def __setProducer(self):
        self.client = Producer({
            'bootstrap.servers': ','.join(self.specs["brokers"])
            })
        return self
    
    def __useMessage(self, message, actionable, action):
        if actionable(message):
            action(message)
            # thread = Thread(target = action, args = (message, ))
            # self.threads.append(thread)
            # thread.start()
        return
    
    def __consume(self, actionable, action):
        stop = False
        # self.threads = []
        while not stop:
            message = self.client.poll(KAFKA.CONSUMER_POLL_TIME)
            if message != None:
                self.__useMessage(message.value().decode("utf-8"), actionable, action)
        # for thread in self.threads:
        #     thread.join()
        self.client.close()
        return
    
    def __produce(self, message):
        self.client.produce(self.specs["topics"][0], message)
        self.client.flush()
        return
    
    def perform(self, message=None, actionable=None, action=None):
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

