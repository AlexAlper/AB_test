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


if __name__ == '__main__':
    with open('query.sql') as f:
        text = f.read()

    print(text)
    exit(0)

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
