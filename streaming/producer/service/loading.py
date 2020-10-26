
import constant.system as CONS
import csv
from time import sleep
import pandas
import constant.batch as BATCH

def readCsv():
    result = []
    with open(CONS.ASSETS["DATA"], 'r') as f:
        reader = csv.reader(f)
        result = list(reader)
    return result

def readData():
    df = pandas.read_csv(CONS.ASSETS["DATA"])
    df = df[BATCH.COLUMNS]
    result = df.values.tolist()
    return result[1:]

def deliver(func):
    data = readData()
    for row in data:
        message = "{}|{}".format(len(data), ','.join([str(r) for r in row]))
        func(**{"message": message})
        sleep(CONS.SLEEP_TIME_DATA)
    return

