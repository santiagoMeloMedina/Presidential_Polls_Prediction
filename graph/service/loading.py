
from configuration.connection import spark
import constant.system as CONS

## Loading csv dataset to pyspark dataframe
dataframe = spark.read.csv(CONS.ASSETS["DATA"], header=True)
