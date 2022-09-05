from calendar import month
from copy import deepcopy
import pandas as pd
from datetime import timedelta, datetime
from dotenv import load_dotenv
import os
from mssql import MsSQL
from time import sleep
import copy
import json
import requests

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

MS_HOST_03 = os.getenv("MS_HOST_VV_03")
MS_USER_03 = os.getenv("MS_USER_VV_03")
MS_PASSWORD_03 = os.getenv("MS_PASSWORD_VV_03")
MS_HOST_06 = os.getenv("MS_HOST_VV_06")
CH_HOST = os.getenv("CH_HOST")
CH_USER = os.getenv("CH_USER")
CH_PASS = os.getenv("CH_PASS")
CH_PORT = os.getenv("CH_PORT")

ms_c_03 = {
        'host': MS_HOST_03,
        'password': MS_PASSWORD_03,
        'user': MS_USER_03,
        'database': 'master'
    }

ms_c_06 = {
        'host': MS_HOST_06,
        'password': MS_PASSWORD_03,
        'user': MS_USER_03,
        'database': 'master'
    }

url = f'http://{CH_USER}:{CH_PASS}@{CH_HOST}:{CH_PORT}'

headers = {
    'X-ClickHouse-Format': 'JSONColumns'
    }

sourсe_06 = MsSQL(
        params=deepcopy(ms_c_06)
    )

QUERY_HEADER = "SELECT * FROM openquery([dwh-clickhouse01],'"
QUERY_TAIL = "');"


def get_numbers_type(test_file):
    df_numbers = pd.read_parquet(test_file)
    df_numbers = df_numbers[['number', 'Type_test']]

    uniqu_type = df_numbers['Type_test'].unique()

    numbers_a = None
    numbers_b = None
    if 'B' in uniqu_type:
        numbers_b = [i.upper().replace('\'','') for i in df_numbers[df_numbers['Type_test'] == 'B']['number'].unique()]
    
    numbers_a = [i.upper().replace('\'','') for i in df_numbers[df_numbers['Type_test'] != 'B']['number'].unique()]

    return [(numbers_a, 'A'),(numbers_b, 'B')]

def get_all_tests(file):
    with open('queries/get_all_test.sql') as f:
        query = f.read()
    
    sourсe = MsSQL(
        params=copy.deepcopy(ms_c_03)
    )

    df = sourсe.select_to_df(query)
    df.to_json(file, indent=4, orient='records')
    os.chmod(file, 0o777)

def get_all_numbers_test(id_test, test_file):
    file_ = os.path.join(f'{os.path.dirname(__file__)}/queries', 'get_all_numbers.sql')
    with open(file_) as f:
        query = f.read()
    
    sourсe = MsSQL(
        params=copy.deepcopy(ms_c_03)
    )
    
    df = sourсe.select_to_df(query.format(id_test=id_test))
    df['Type_test'] = df['Type_test'].str.strip()
    df.to_parquet(test_file)
    os.chmod(test_file, 0o777)


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

def get_time_on_days(date_begin, date_end):
    days = []
    while date_begin < date_end:
        days.append((date_begin, date_begin + timedelta(days=1)))
        date_begin = date_begin + timedelta(days=1)

    return days


def get_metrics(id_test, test_file, days_):
    
    numbers_list = get_numbers_type(test_file)

    df_full = pd.DataFrame()

    with open('queries/query.json') as f:
        j_queries = json.load(f)

    for number_list, test_type in numbers_list:
        if number_list:
            for number, i in enumerate([q for q in j_queries if len(q['querys']) == 1][4:5]):
                answer = 0
                metric = i['metrics'][0]
                query_main_list = i['querys']
                source = i['source']
                type_q = i['type_q']
                q = query_main_list[0].strip()
                q_search = q.lower()
                df_metrics = pd.DataFrame()
                if q_search.find('{number_list}') == -1:
                    step = len(number_list)
                else:
                    step = 10000
                for date_from, date_to in days_:
                    file_tests = os.path.join(f'{os.path.dirname(__file__)}/all_answers/{id_test}', f"{date_from.strftime('%Y%m%d')}_{date_to.strftime('%Y%m%d')}_{test_type}_{metric}.parquet")
                    if not os.path.exists(file_tests):
                        df_new = pd.DataFrame()
                        for page in range(0,len(number_list),step):
                            page_query = number_list[page:page+step]
                            query = ''
                            if  q != '':
                                if source == 'MS':
                                    query = q.format(
                                            date_1=date_from.strftime('%Y%m%d'), 
                                            date_2=date_to.strftime('%Y%m%d'), 
                                            number_list=str(tuple(page_query)).replace(",)",")")
                                        )

                                    print(query)

                                    if df_new.empty:
                                        df_new = sourсe_06.select_to_df(query)
                                    else:
                                        df_c = sourсe_06.select_to_df(query)
                                        if not df_c.empty:
                                            df_new = pd.concat([df_new,df_c])
                                else:
                                    query = q.format(
                                            date_1=date_from.strftime('%Y-%m-%d'), 
                                            date_2=date_to.strftime('%Y-%m-%d'), 
                                            number_list=str(tuple(page_query)).replace(",)",")")
                                        )

                                    http = requests.post(
                                        url=url, 
                                        data=query, 
                                        headers=headers
                                        )

                                    if http.status_code != 200:
                                        continue

                                    if df_new.empty:
                                        df_new = pd.DataFrame(json.loads(http.text))
                                    else:
                                        df_c = pd.DataFrame(json.loads(http.text))
                                        if not df_c.empty:
                                            df_new = pd.concat([df_new,df_c])

                        if not df_new.empty:
                            if type_q == 'unique':  
                                df_new = df_new.drop_duplicates()
                                answer = float(len(df_new))
                            elif type_q == 'avg':
                                df_new = df_new.astype('int')
                                answer = float(df_new['value'].mean())
                            elif type_q == 'count':  
                                df_new = df_new.astype('int')
                                answer = float(df_new['value'].sum())

                        df_new = pd.DataFrame([round(answer, 2)], columns=[f'type_{test_type}'], index=[metric])
                        df_new['date'] = date_from
                        df_new.to_parquet(file_tests)
                        print(df_new)
                        os.chmod(file_tests, 0o777)



def get_metrics_all(days_):

    df_full = pd.DataFrame()

    with open('queries/query.json') as f:
        j_queries = json.load(f)

    for number, i in enumerate(j_queries):
        answer = 0
        metric = i['metrics'][0]
        query_main_list = i['querys']
        source = i['source']
        type_q = i['type_q']
        q = query_main_list[0].strip()
        q = q.replace(' AND number in {number_list}', '')
        q_search = q.lower()
        df_metrics = pd.DataFrame()
        for date_from, date_to in days_:
            file_tests = os.path.join(f'{os.path.dirname(__file__)}/all_answers/all_numbers', f"{date_from.strftime('%Y%m%d')}_{date_to.strftime('%Y%m%d')}_{metric}.parquet")
            if not os.path.exists(file_tests):
                df_new = pd.DataFrame()
                query = ''
                if  q != '':
                    if source == 'MS':
                        query = q.format(
                                date_1=date_from.strftime('%Y%m%d'), 
                                date_2=date_to.strftime('%Y%m%d')
                            )

                        if df_new.empty:
                            df_new = sourсe_06.select_to_df(query)
                        else:
                            df_c = sourсe_06.select_to_df(query)
                            if not df_c.empty:
                                df_new = pd.concat([df_new,df_c])
                    else:
                        query = q.format(
                                date_1=date_from.strftime('%Y-%m-%d'), 
                                date_2=date_to.strftime('%Y-%m-%d')
                            )

                        http = requests.post(
                            url=url, 
                            data=query, 
                            headers=headers
                            )

                        if http.status_code != 200:
                            continue

                        if df_new.empty:
                            df_new = pd.DataFrame(json.loads(http.text))
                        else:
                            df_c = pd.DataFrame(json.loads(http.text))
                            if not df_c.empty:
                                df_new = pd.concat([df_new,df_c])

                if not df_new.empty:
                    if type_q == 'unique':  
                        df_new = df_new.drop_duplicates()
                        answer = float(len(df_new))
                    elif type_q == 'avg':
                        df_new = df_new.astype('int')
                        answer = float(df_new['value'].mean())
                    elif type_q == 'count' or type_q == 'sum':  
                        df_new = df_new.astype('int')
                        answer = float(df_new['value'].sum())
                            
                df_new = pd.DataFrame(
                    {
                        'value': [round(answer, 2)],
                        'index': [metric],
                        'date': [date_from]
                    }
                )

                df_new.set_index(['date', 'index'], inplace=True)
                
                if not df_new.empty:
                    df_new.to_parquet(file_tests)
                    print(df_new)
                    os.chmod(file_tests, 0o777)



# def get_metrics_complex(id_test, test_file, days_):
    
#     numbers_list = get_numbers_type(test_file)

#     df_full = pd.DataFrame()

#     with open('queries/query.json') as f:
#         j_queries = json.load(f)

#     for line_metric in [q for q in j_queries if len(q['querys']) > 1]:
#         for number_list, test_type in numbers_list:
#             answer = 0
#             metric = line_metric['metrics'][0]
#             query_list = line_metric['querys']
#             source = line_metric['source'].split('/')
#             type_q = line_metric['type_q'].split('/')
#             df_metrics = pd.DataFrame()
#             for query in query_list:
#                 for date_from, date_to in days_:
#                     for action in type_q:
#                         if action == 'select':
                            
#                             # for num ,sor in enumerate(source):
                                

#                         elif action == 'sum':

#                                 # if not os.path.exists(file_tests):
#                                 #     df_new = pd.DataFrame()
#                                 #     query = ''
#                                 #     if  q != '':
#                                 #         if source == 'MS':
#                                 #             query = q.format(
#                                 #                     date_1=date_from.strftime('%Y%m%d'), 
#                                 #                     date_2=date_to.strftime('%Y%m%d')
#                                 #                 )

#                                 #             if df_new.empty:
#                                 #                 df_new = sourсe_06.select_to_df(query)
#                                 #             else:
#                                 #                 df_c = sourсe_06.select_to_df(query)
#                                 #                 if not df_c.empty:
#                                 #                     df_new = pd.concat([df_new,df_c])
#                                 #         else:
#                                 #             query = q.format(
#                                 #                     date_1=date_from.strftime('%Y-%m-%d'), 
#                                 #                     date_2=date_to.strftime('%Y-%m-%d')
#                                 #                 )

#                                 #             http = requests.post(
#                                 #                 url=url, 
#                                 #                 data=query, 
#                                 #                 headers=headers
#                                 #                 )

#                                 #             if http.status_code != 200:
#                                 #                 continue

#                                 #             if df_new.empty:
#                                 #                 df_new = pd.DataFrame(json.loads(http.text))
#                                 #             else:
#                                 #                 df_c = pd.DataFrame(json.loads(http.text))
#                                 #                 if not df_c.empty:
#                                 #                     df_new = pd.concat([df_new,df_c])