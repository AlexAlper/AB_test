import pyodbc
import pandas as pd

QUERY_HEADER = "select * from openquery([dwh-clickhouse01],'"
QUERY_TAIL = "');"


def create_file(df: pd.DataFrame, file_name: str) -> str:
    if len(df) == 0:
        return None
    try:
        file_name = f'{file_name}.parquet'
        df.to_parquet(file_name)
        print(f'created {file_name}')
        return file_name
    except Exception as err:
        print(err, ':', file_name)
        return None


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


if __name__ == '__main__':
    with open('query.sql') as f:
        text = f.read()

    ms_c = {
        'host': ms_connect.host,
        'password': ms_connect.password,
        'login': ms_connect.login,
        'database': ms_connect.schema
    }

    sourсe = MsSQL(
        params=ms_c
    )

    querys = text.split(';')
    for i in range(0, len(querys), 2):
        query = QUERY_HEADER + querys[i] + QUERY_TAIL
        file_name = querys[i - 1]
        if query != '':
            sourсe.select_to_df(query)
