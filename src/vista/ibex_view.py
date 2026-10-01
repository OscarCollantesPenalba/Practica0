class IbexView:
    """Capa de vista: solo imprime por consola, no calcula ni transforma datos."""

    def titulo(self, ejercicio):
        print(ejercicio)

    def esquema(self, df):
        df.printSchema()

    def filas(self, df, n=6):
        df.show(n)