class IbexView:
    """Solo muestra información por consola (no calcula nada)."""

    def title(self, ejercicio):
        print(ejercicio)

    def schema(self, df):
        df.printSchema()

    def rows(self, df, n=6):
        df.show(n)
