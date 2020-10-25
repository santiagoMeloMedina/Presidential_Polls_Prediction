
import os

os.environ["SPARKIP"] = os.environ.get("SPARKIP") if os.environ.get("SPARKIP") else "spark://VivoBook-X412DA-X412DA:7077"

SPARK_MASTER_IP = os.environ.get("SPARKIP")
SPARK_CONN_NAME = "PresidentialPolls"

ASSETS = {
    "DATA": "./assets/data.csv"
}