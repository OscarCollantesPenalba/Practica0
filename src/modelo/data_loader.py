import os
from src.modelo.spark_session import BASE_DIR

CSV_PATH = os.path.join(BASE_DIR, "data", "ibex35_close-2024.csv")


def load_raw(spark):
    """Carga el CSV tal cual (todas las columnas como string)."""
    return (spark.read
            .option("header", True)
            .option("sep", ";")
            .option("dateFormat", "dd/MM/yyyy")
            .csv(CSV_PATH))


def load_with_schema(spark, schema):
    """Carga el CSV aplicando un esquema explícito (tipos y nombres ya correctos)."""
    return (spark.read
            .option("header", True)
            .option("sep", ";")
            .option("dateFormat", "dd/MM/yyyy")
            .schema(schema)
            .csv(CSV_PATH))