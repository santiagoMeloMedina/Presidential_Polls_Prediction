
from pyspark.sql import SparkSession
import constant.system as CONS
from neo4j import GraphDatabase

## Connection made with spark service on spark master process
spark = SparkSession.builder \
    .master(CONS.SPARK_MASTER_IP) \
    .appName(CONS.SPARK_CONN_NAME).getOrCreate()

neo4j = GraphDatabase.driver(CONS.NEO4J_URI, auth=(CONS.NEO4J_USERNAME, CONS.NEO4J_PASSWORD)) 
