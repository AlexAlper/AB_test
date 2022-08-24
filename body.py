from calendar import month
import pandas as pd
from datetime import timedelta, datetime
from dotenv import load_dotenv
import os
from mssql import MsSQL
from time import sleep
import copy
import json

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

MS_HOST_03 = os.getenv("MS_HOST_VV_03")
MS_USER_03 = os.getenv("MS_USER_VV_03")
MS_PASSWORD_03 = os.getenv("MS_PASSWORD_VV_03")
MS_HOST_06 = os.getenv("MS_HOST_VV_06")


ms_c_03 = {
        'host': MS_HOST_03,
        'password': MS_PASSWORD_03,
        'user': MS_USER_03,
        'database': 'master'
    }


QUERY_HEADER = "SELECT * FROM openquery([dwh-clickhouse01],'"
QUERY_TAIL = "');"

def get_all_tests():
    with open('queries/get_all_test.sql') as f:
        query = f.read()
    
    sourсe = MsSQL(
        params=copy.deepcopy(ms_c_03)
    )

    df = sourсe.select_to_df(query)
    df.to_json('all_answers/all_tests.json', indent=4, orient='records')

def get_all_numbers_test(id_test):
    with open('queries/get_all_numbers.sql') as f:
        query = f.read()
    
    sourсe = MsSQL(
        params=copy.deepcopy(ms_c_03)
    )

    df = sourсe.select_to_df(query.format(id_test=id_test))
    df.to_parquet(f'all_answers/{id_test}_number.parquet')


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
    # sleep(2)
    # id_test=4

    # test_number = os.path.join(f'{os.path.dirname(__file__)}/all_answers', '{id_test}_number.parquet')
    # if not os.path.exists(test_number) and os.path.getctime(all_tests) < datetime.now():
    #     get_all_numbers_test(id_test=id_test)

    # exit(0)

    with open('queries/query.json') as f:
        text = f.read()

    j_queries = json.loads(text)

    ms_c = {
        'host': MS_HOST_03,
        'password': MS_PASSWORD_03,
        'user': MS_USER_03,
        'database': 'master'
    }

    # sourсe = MsSQL(
    #     params=ms_c
    # )

    date_begin = datetime.now()
    date_end = datetime.now() + timedelta(days=1)
    number_list = ['asdasd', 'adqwdf', 'asdrebrt']
    number_list = "(''" + "'',''".join(number_list) + "'')"
    querys = text.split(';')
    for i in j_queries:
        # metric_name = i['metrics']
        query_main = i['query'].strip()
        print(query_main)
        # query = ''
        # if  query_main != '':
        #     print('\n', metric_name)
        #     if metric_name.find('MS') > 0:
        #         query = query.format(year_1=date_begin.year, month_1=date_begin.month, day_1=date_begin.day, year_2=date_end.year, month_2=date_end.month, day_2=date_end.day)
        #     else:
        #         query = QUERY_HEADER + query_main + QUERY_TAIL
        #         query = query.format(date_1=date_begin.strftime('%Y-%m-%d'), date_2=date_end.strftime('%Y-%m-%d'), number_list=number_list)
        #     try:
        #         print(query)
        #         # df = sourсe.select_to_df(query)
        #         sleep(1)
        #         print(df)
        #     except Exception as err:
        #         print(err)
