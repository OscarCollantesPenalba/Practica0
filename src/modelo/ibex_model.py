from pyspark.sql.functions import col, regexp_replace, min, max, countDistinct

from src.modelo.data_loader import load_raw, load_with_schema
from src.modelo.ibex_schema import build_schema


class IbexModel:
    """Acceso y transformación de los datos del IBEX-35 (no imprime nada)."""

    def __init__(self, spark):
        self.spark = spark

    def load_raw(self):
        return load_raw(self.spark)

#========================================================================================
#===================Ejercicio 1==========================================================
#========================================================================================

    def convert_types(self, df):
        """Ej1-a: Fecha (string -> date) y precios (string -> decimal)."""
        # dd/MM/yyyy -> yyyy-MM-dd (formato que necesita cast("date"))
        df = df.withColumn("Fecha", regexp_replace(col("Fecha"), r"(\d{2})/(\d{2})/(\d{4})", r"$3-$2-$1"))
        df = df.withColumn("Fecha", col("Fecha").cast("date"))
        # Los nombres como "IBE.MC" llevan punto: dentro de col() van entre acentos graves
        for c in df.columns:
            if c != "Fecha":
                df = df.withColumn(c, col(f"`{c}`").cast("decimal(10, 2)"))
        return df

    def remove_suffix(self, df, suffix=".MC"):
        """Ej1-b: quita el sufijo de los nombres de todas las columnas."""
        for c in df.columns:
            df = df.withColumnRenamed(c, c.replace(suffix, ""))
        return df

    def load_with_schema(self):
        """Ej1-c: carga directa con StructType (tipos y nombres de empresa)."""
        return load_with_schema(self.spark, build_schema())


#========================================================================================
#===================Ejercicio 2==========================================================
#========================================================================================

    def clean_data(self, df):
        """Ej2-a: elimina filas duplicadas y filas sin información."""
        df = df.dropDuplicates()
        empresas = [c for c in df.columns if c != "Fecha"]
        df = df.dropna(how="all", subset=empresas)
        return df
    
    def periodo(self, df):
        """Ej2-b: fecha inicial, fecha final y número de días con datos."""
        fila = df.select(min("Fecha"), max("Fecha")).head(1)[0]
        dias = df.select(countDistinct("Fecha")).head(1)[0][0]
        return fila[0], fila[1], dias
    

