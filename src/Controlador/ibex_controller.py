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