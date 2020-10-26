
import constant.system as CONS
import csv
from time import sleep

def readCsv():
    result = []
    with open(CONS.ASSETS["DATA"], 'r') as f:
        reader = csv.reader(f)
        result = list(reader)
    return result

def deliver(func):
    data = readCsv()
    for row in data:
        message = "{}|{}".format(len(data), ','.join(row))
        func(**{"message": message})
        sleep(CONS.SLEEP_TIME_DATA)
    return

