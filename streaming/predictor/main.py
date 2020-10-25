
from configuration.kafka import consumer
from time import sleep
from model.batch import Batch

def main():
    batch = Batch()
    consumer.perform(**{"actionable": batch.addRows, "action": batch.action})

if __name__ == "__main__":
    main()
