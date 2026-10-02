import os


class DBConnection:
    """Gestiona la conexión JDBC entre Spark y MySQL."""

    def __init__(self, host="localhost", port=3306, database="IBEX35", user="Oscar", password="1234"):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password

    @property
    def url(self):
        return f"jdbc:mysql://{self.host}:{self.port}/{self.database}"

    @property
    def properties(self):
        return {
            "driver": "com.mysql.cj.jdbc.Driver",
            "user": self.user,
            "password": self.password,
        }

    def write_table(self, data_frame, table, mode="overwrite"):
        """Guarda un DataFrame como tabla en la base de datos."""
        data_frame.write.jdbc(url=self.url, table=table, mode=mode, properties=self.properties)

    def read_table(self, spark, table):
        """Lee una tabla de la base de datos como DataFrame."""
        return spark.read.jdbc(url=self.url, table=table,properties=self.properties)