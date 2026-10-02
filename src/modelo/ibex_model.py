from pyspark.sql.functions import col, regexp_replace, min, max, countDistinct, avg, when,lag
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DecimalType
 
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
        df = df.withColumn("Fecha", regexp_replace(col("Fecha"), r"(\d{2})/(\d{2})/(\d{4})", r"$3-$2-$1"))
        df = df.withColumn("Fecha", col("Fecha").cast("date"))
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
        return df.orderBy("Fecha")
    
    def periodo(self, df):
        """Ej2-b: fecha inicial, fecha final y número de días con datos."""
        fila = df.select(min("Fecha"), max("Fecha")).head(1)[0]
        dias = df.select(countDistinct("Fecha")).head(1)[0][0]
        return fila[0], fila[1], dias
    
#========================================================================================
#===================Ejercicio 3==========================================================
#========================================================================================

    def rename_fecha(self, df):
        """Ej3: renombra la columna Fecha a Dia."""
        return df.withColumnRenamed("Fecha", "Dia")
 
    def estadisticas_anuales(self, df):
        """Ej3: Crea un dataframe con la media, maximo y minimo intervencion de las empresas"""
        empresas = [c for c in df.columns if c != "Dia"]
        medias = df.agg({c: "avg" for c in empresas}).head(1)[0]
        maximos = df.agg({c: "max" for c in empresas}).head(1)[0]
        minimos = df.agg({c: "min" for c in empresas}).head(1)[0]
        datos = [(c, round(medias[f"avg({c})"], 2), maximos[f"max({c})"], minimos[f"min({c})"])
                for c in empresas]
        esquema = StructType([
            StructField("Empresa", StringType(), True),
            StructField("Media anual", DecimalType(10, 2), True),
            StructField("Max anual", DecimalType(10, 2), True),
            StructField("Min anual", DecimalType(10, 2), True),
        ])
        return self.spark.createDataFrame(datos, schema=esquema)
 
    def add_deficiency_notice(self, df):
        """Ej3: True si UNI cierra por debajo de 1 EUR ese día (cada día por separado)."""
        return df.withColumn("Deficiency Notice UNI", when(col("UNI") < 1, True).otherwise(False))
    
#========================================================================================
#===================Ejercicio 4==========================================================
#========================================================================================

    def variacion_anual(self, df):
        """Ej4: variación entre el primer y el último día y clasificación de cada empresa."""
        empresas = [c for c in df.columns if c != "Dia"]
        fechas = df.select(min("Dia"), max("Dia")).head(1)[0]
        inicial = df.filter(col("Dia") == fechas[0]).head(1)[0]
        final = df.filter(col("Dia") == fechas[1]).head(1)[0]
        datos = [(c, inicial[c], final[c]) for c in empresas]
        esquema = StructType([
            StructField("Empresa", StringType(), True),
            StructField("Inicial", DecimalType(10, 2), True),
            StructField("Final", DecimalType(10, 2), True),
        ])
        resultado = self.spark.createDataFrame(datos, schema=esquema)
        # Solo se pueden calcular las empresas con precio el primer y el último día
        resultado = resultado.dropna(subset=["Inicial", "Final"])
        resultado = resultado.withColumn("Variación Anual", (col("Final") - col("Inicial")) / col("Inicial") * 100)
        resultado = resultado.withColumn(
            "Clasificación",
            when(col("Variación Anual") <= -15, "Bajada Fuerte")
            .when(col("Variación Anual") < -1, "Bajada")
            .when(col("Variación Anual") <= 1, "Neutra")
            .when(col("Variación Anual") < 15, "Subida")
            .otherwise("Subida Fuerte"))
        return resultado.withColumn("Variación Anual", col("Variación Anual").cast("decimal(10, 2)"))


#========================================================================================
#===================Ejercicio 5==========================================================
#========================================================================================

    def add_cuartiles(self, df):
        """Ej5: para cada empresa añade la columna <Empresa>Cuartil (q1, q2, q3 o q4) segun
        en que cuartil de su distribucion de precios cae el valor de cada sesion."""
        empresas = [c for c in df.columns if c != "Dia"]
        cuartiles = df.approxQuantile(empresas, [0.25, 0.5, 0.75], 0.01)
        nuevas = []
        for empresa, q in zip(empresas, cuartiles):
            q1, q2, q3 = q
            precio = col(empresa)
            nuevas.append(
                when(precio.isNotNull(),
                     when(precio <= q1, "q1")
                     .when(precio <= q2, "q2")
                     .when(precio <= q3, "q3")
                     .otherwise("q4"))
                .alias(f"{empresa}Cuartil"))
        return df.select("*", *nuevas)

#========================================================================================
#===================Ejercicio 6==========================================================
#========================================================================================

    def add_cambio_significativo(self, df):
        """Ej6: para cada empresa añade <Empresa>CambioSignificativo con la variacion (%) respecto
        a la sesion anterior si es mayor al 8 % en valor absoluto, y "-" en caso contrario."""
        empresas = [c for c in df.columns if c != "Dia"]
        ventana = Window.orderBy("Dia")
        nuevas = []
        for empresa in empresas:
            anterior = lag(col(empresa)).over(ventana)
            variacion = (col(empresa) - anterior) / anterior * 100
            # Valor absoluto > 8  <=>  variacion > 8 o variacion < -8.
            # Si no hay dato (primer dia o precio NULL) la condicion no se cumple y sale "-"
            nuevas.append(
                when((variacion > 8) | (variacion < -8), variacion.cast("decimal(10, 2)").cast("string"))
                .otherwise("-")
                .alias(f"{empresa}CambioSignificativo"))
        return df.select("*", *nuevas)

    def fila(self, df, n):
        """Devuelve la fila n (empezando en 1) como un DataFrame de una sola fila."""
        return self.spark.createDataFrame([df.head(n)[n - 1]], df.schema)