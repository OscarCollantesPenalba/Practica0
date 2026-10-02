import os
import sys
from pyspark.sql import SparkSession

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
JAR_PATH = os.path.join(BASE_DIR, "lib", "mysql-connector-j-26.7.0.jar")


def get_spark_session(app_name="IBEX35"):
    # Spark lanza procesos Python en algunas operaciones (createDataFrame, UDF...).
    # Se le indica el mismo Python que ejecuta este script, para que no busque "python3"
    # en el PATH (en Windows resuelve al acceso directo de la Microsoft Store).
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (SparkSession.builder
             .appName(app_name)
             .config("spark.driver.extraClassPath", JAR_PATH)
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark