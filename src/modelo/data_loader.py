import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(BASE_DIR, "data", "ibex35_close-2024.csv")  # pon el nombre real de tu CSV

def load_raw(spark):
    return (spark.read
            .option("header", True)
            .option("sep", ";")
            .csv(CSV_PATH))