
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType
import constant.batch as BATCH
from configuration.connection import spark
from model.pollsData import PollsData
import math

class Batch:
    def __init__(self):
        self.data = []
        self.__setSchema()
        self.size = None
        self.result = 0
    
    def __setSchema(self):
        schema = []
        for name, type in BATCH.COLUMNS:
            schema.append(StructField(name, type, True))
        self.schema = StructType(schema)
        return self
    
    def getAsDataframe(self):
        df = spark.createDataFrame(self.data, schema=self.schema)
        df.show()
        return df
    
    def __castData(self, data):
        result = []
        rows = data.split('@')
        for row in rows:
            tmp = []
            for column in row.split(','):
                try: 
                    number = float(column)
                    tmp.append(number if not math.isnan(number) else 0.0)
                except: tmp.append(column)
            result.append(tmp)
        return result

    def addRows(self, data):
        size, data = data.split('|')
        self.size = float(size)
        self.data += self.__castData(data)
        return self.actionable()
    
    def actionable(self):
        result = False
        if self.size != None:
            result = 100/self.size*len(self.data) >= BATCH.ACCEPTER_BATCH_PERCENTAGE
        print("Captured {:.2f}% of data".format(100/self.size*len(self.data)), end="\r")
        return result
    
    def action(self, message):
        self.result += 1
        polls = PollsData(self.getAsDataframe())
        polls.cleanData()
        polls.prediction()
        polls.output("result#{}".format(self.result))
    
    def reset(self):
        self.data = []
        self.size = None
        return self
