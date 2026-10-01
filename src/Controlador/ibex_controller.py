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

    def ej1a(self):
        # Ej1-a
        # Herramienta IA usada: Claude
        self.view.titulo("Ej1-a")
        df = self.model.load_raw()
        self.view.esquema(df)
        self.df = self.model.convert_types(df)
        self.view.esquema(self.df)
        self.view.filas(self.df, 6)

    def ej1b(self):
        # Ej1-b
        # Herramienta IA usada: Claude
        self.view.titulo("Ej1-b")
        self.df = self.model.remove_suffix(self.df)
        self.view.filas(self.df, 6)

    def ej1c(self):
        # Ej1-c (opcional)
        # Herramienta IA usada: Claude
        self.view.titulo("Ej1-c")
        df_schema = self.model.load_with_schema()
        self.view.esquema(df_schema)
        self.view.filas(df_schema, 6)

    def ej2a(self):
        # Ej2-a
        # Herramienta IA usada: Claude
        self.view.titulo("Ej2-a")
        filas_antes = self.df.count()
        self.df = self.model.clean_data(self.df)
        filas_despues = self.df.count()
        self.view.valor("Filas eliminadas", filas_antes - filas_despues)
        self.view.valor("Empresas con información", len(self.df.columns) - 1)

    def ej2b(self):
        # Ej2-b
        # Herramienta IA usada: Claude
        self.view.titulo("Ej2-b")
        inicio, fin, dias = self.model.periodo(self.df)
        self.view.valor("Fecha inicial", inicio)
        self.view.valor("Fecha final", fin)
        self.view.valor("Días con información", dias)
        # Texto orientativo: contrasta los días que faltan con el calendario de BME y reescríbelo con tus palabras
        self.view.mensaje("Comentario: el periodo cubre todo 2024 con 255 sesiones, coherente con un año bursátil "
                          "(262 días laborables menos 7 sin datos, que parecen festivos de mercado).")
        self.view.mensaje("No parece necesario buscar más datos, aunque conviene confirmar en el calendario de BME "
                          "que el 31/12 no fue sesión.")