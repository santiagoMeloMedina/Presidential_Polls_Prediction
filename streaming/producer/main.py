
from configuration.kafka import producer
from service.loading import deliver


def main():
    deliver(producer.perform)

if __name__ == "__main__":
    main()