
import constant.system as CONS
import csv
from time import sleep
import pandas
import constant.batch as BATCH

def readCsv():
    """ This function reads the data from the dataset and converts it 
        to a list"""
    result = []
    with open(CONS.ASSETS["DATA"], 'r') as f:
        reader = csv.reader(f)
        result = list(reader)
    return result

def readData():
    """ This function chooses the specified columns from the dataset 
        and returns the rows"""
    df = pandas.read_csv(CONS.ASSETS["DATA"])
    df = df[BATCH.COLUMNS]
    result = df.values.tolist()
    return result[1:]

def deliver(func):
    """ This function uses the consumer client tom send the rows every 
        specified time"""
    data = readData()
    for row in data:
        message = "{}|{}".format(len(data), ','.join([str(r) for r in row]))
        func(**{"message": message})
        sleep(CONS.SLEEP_TIME_DATA)
    return

