from src.modelo.spark_session import get_spark_session
from src.modelo.ibex_model import IbexModel
from src.vista.ibex_view import IbexView


class IbexController:
    """Orquesta los ejercicios: pide datos al modelo y se los pasa a la vista."""

    def __init__(self):
        self.spark = get_spark_session()
        self.model = IbexModel(self.spark)
        self.view = IbexView()
