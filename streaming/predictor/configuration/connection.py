
from pyspark.sql import SparkSession
import constant.system as CONS

## Connection made with spark service on spark master process
spark = SparkSession.builder \
    .master(CONS.SPARK_MASTER_IP) \
    .appName(CONS.SPARK_CONN_NAME).getOrCreate()