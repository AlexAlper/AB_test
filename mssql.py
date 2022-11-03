import pyodbc
import pandas as pd


class MsSQL:
    def __init__(self, **kwargs):
        """

        :param kwargs:

        host:
        password:
        login:
        database:

        """
        conn = kwargs.pop('params')
        self.host = conn.pop('host')
        self.__password = conn.pop('password')
        self.__login = conn.pop('user')
        self.database = conn.pop('database')

        self.conn_ms = "DRIVER={ODBC Driver 17 for SQL Server};" \
                       f"SERVER={self.host}" \
                       f";DATABASE={self.database}" \
                       f";UID={self.__login}" \
                       f";PWD={self.__password}"

    def select_to_df(self, query) -> pd.DataFrame:
        """
        получение данных в формате DataFrame
        :return: DataFrame
        """

        with pyodbc.connect(self.conn_ms) as cnxn:
            return pd.read_sql(query, cnxn)
