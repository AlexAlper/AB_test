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
        self.__login = conn.pop('login')
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

        self.print_select(query)
        with pyodbc.connect(self.conn_ms) as cnxn:
            return pd.read_sql(query, cnxn)

    def select_to_dict(self, query) -> list:
        """
        получение данных в list of dict
        :return: list
        """

        self.print_select(query)
        with pyodbc.connect(self.conn_ms) as cnxn:
            df = pd.read_sql(query, cnxn)
            return df.to_dict('records')
