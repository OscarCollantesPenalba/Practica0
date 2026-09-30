import os
from pyspark.sql import SparkSession

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JAR_PATH = os.path.join(BASE_DIR, "lib", "mysql-connector-j-26.7.0.jar")  

def get_spark_session(app_name="IBEX35"):
    return (SparkSession.builder
            .appName(app_name)
            .config("spark.driver.extraClassPath", JAR_PATH)
            .getOrCreate())