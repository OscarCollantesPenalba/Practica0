from src.modelo.spark_session import get_spark_session
from src.modelo.ibex_model import IbexModel
from src.vista.ibex_view import IbexView


class IbexController:
    """Orquesta los ejercicios: pide datos al modelo y se los pasa a la vista."""

    def __init__(self):
        self.spark = get_spark_session()
        self.model = IbexModel(self.spark)
        self.view = IbexView()
        self.df = None  # DataFrame de trabajo, se va actualizando ejercicio a ejercicio

    def run(self):
        self.ej1a()
        self.ej1b()
        self.ej1c()
        self.ej2a()
        self.ej2b()
        self.ej3a()
        self.ej3b()
        self.ej4()
        self.ej5()

    def ej1a(self):
        # Ej1-a
        self.view.titulo("Ej1-a")
        df = self.model.load_raw()
        self.view.esquema(df)
        self.df = self.model.convert_types(df)
        self.view.esquema(self.df)
        self.view.filas(self.df, 6)

    def ej1b(self):
        # Ej1-b
        self.view.titulo("Ej1-b")
        self.df = self.model.remove_suffix(self.df)
        self.view.filas(self.df, 6)

    def ej1c(self):
        # Ej1-c (opcional)
        self.view.titulo("Ej1-c")
        df_schema = self.model.load_with_schema()
        self.view.esquema(df_schema)
        self.view.filas(df_schema, 6)

    def ej2a(self):
        # Ej2-a
        self.view.titulo("Ej2-a")
        filas_antes = self.df.count()
        self.df = self.model.clean_data(self.df)
        filas_despues = self.df.count()
        self.view.valor("Filas eliminadas", filas_antes - filas_despues)
        self.view.valor("Empresas con información", len(self.df.columns) - 1)

    def ej2b(self):
        # Ej2-b
        self.view.titulo("Ej2-b")
        inicio, fin, dias = self.model.periodo(self.df)
        self.view.valor("Fecha inicial", inicio)
        self.view.valor("Fecha final", fin)
        self.view.valor("Días con información", dias)
        self.view.mensaje("Comentario: el periodo cubre todo 2024 con 255 sesiones, coherente con un año bursátil "
                          "(262 días laborables menos 7 sin datos, que parecen festivos de mercado).")
    
    def ej3a(self):
        # Ej3-a
        self.view.titulo("Ej3")
        self.df = self.model.rename_fecha(self.df)
        self.view.filas(self.df, 10)
        estadisticas = self.model.estadisticas_anuales(self.df)
        self.view.filas(estadisticas, estadisticas.count())
        df_deficiency = self.model.add_deficiency_notice(self.df)
        self.view.filas(df_deficiency, 100)
        
    def ej3b(self):
        #Ej3-b
        #Herramienta IA usda: Claude
        self.view.titulo("Ej3-b")
        self.view.mensaje("Comentario/Reflexión/Investigación: las empresas son las que formaron parte del IBEX 35 en 2024; "
                        "el Comité Asesor Técnico de BME las elige sobre todo por liquidez (volumen negociado en euros) "
                        "y por superar una capitalización mínima, no por su tamaño.")
        self.view.mensaje("Los NULL de MEL y PUIG vienen del cambio de composición del índice: Puig sustituyó a Meliá "
                        "el 22/07/2024, y cada una solo tiene datos mientras estuvo en el IBEX 35.")
        self.view.mensaje("Afectan solo al Ej3: avg, max y min ignoran los NULL, así que MEL y PUIG se calculan con "
                        "141 y 114 días de 255, no con todo el año.")
        self.view.mensaje("Referencia/s: CNMV, infografía del IBEX 35 (https://www.cnmv.es/DocPortal/Publicaciones/Infografias/IBEX35.pdf); "
                        "Valencia Plaza, 'Puig entrará en el Ibex 35 a partir del 22 de julio', julio de 2024 "
                        "(https://valenciaplaza.com/puig-entrara-ibex-35-partir-22-julio); "
                        "INEAF, 'Qué es el IBEX 35' (https://www.ineaf.es/tribuna/que-es-ibex-35/).")
        
    def ej4(self):
        # Ej4
        # Herramienta IA usada: Claude
        self.view.titulo("Ej4")
        variaciones = self.model.variacion_anual(self.df)
        self.view.filas(variaciones, variaciones.count())
        
    def ej5(self):
        #Ej5
        self.view.titulo("Ej5")
        df_cuartiles = self.model.add_cuartiles(self.df)
        self.view.filas(df_cuartiles,1)
        aena_bbva = df_cuartiles.select("Dia", "AENA", "AENACuartil", "BBVA", "BBVACuartil")
        self.view.filas(aena_bbva, aena_bbva.count())