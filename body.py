from calendar import month
from copy import deepcopy
from statistics import quantiles
import pandas as pd
from datetime import timedelta, datetime
from dotenv import load_dotenv
import os
from mssql import MsSQL
from time import sleep
import copy
import json
import requests
import glob
from test123 import DataManipulation

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
PG_HOST = os.getenv("POSTGRES_HOST")
PG_PASS = os.getenv("POSTGRES_PASSWORD")
PG_USER = os.getenv("POSTGRES_USER")
PG_SCHEMA = os.getenv("POSTGRES_DB")

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



def get_all_numbers_test(id_test, test_file, numbers_len=None):
    file_ = os.path.join(f'{os.path.dirname(__file__)}/queries', 'get_all_numbers.sql')
    with open(file_) as f:
        query = f.read()

    if numbers_len:
        len_num = len(numbers_len[0])
        query = query.replace('id_ab_test = {id_test}', 'id_ab_test = {id_test}' + f' AND CAST(SUBSTRING(number,{7-len_num},7) AS integer) between {int(numbers_len[0])} and {int(numbers_len[1])}')
    
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


def get_numbers_path(number_path):
    if number_path:
        number_list_path = f'/{number_path[0]}_{number_path[1]}'
    else:
        number_list_path = ''

    return number_list_path


def select_from_bd(query, source):
    df_new = pd.DataFrame()

    if source == 'MS':
        df_new = sourсe_06.select_to_df(query)
    else:
        http = requests.post(
            url=url, 
            data=query, 
            headers=headers
            )

        if http.status_code != 200:
            return df_new

        df_new = pd.DataFrame(json.loads(http.text))
    
    return df_new


def replase_querys(id_test, numbers_len=None):

    with open('queries/query.json') as f:
        j_queries = json.load(f)

    for instruc in j_queries:
        querys = instruc['querys']
        for q in querys:
            if numbers_len:
                len_num = len(numbers_len[0])
                if q['source'] == 'CH': 
                    q['query'] = q['query'].replace('AND number in {number_list}', 'AND number in {number_list}' + f' AND toInt32OrZero(substring(number,{7-len_num+1},7)) between {int(numbers_len[0])} and {int(numbers_len[1])}')
                if q['source'] == 'MS': 
                    q['query'] = q['query'].replace('AND number in {number_list}', 'AND number in {number_list}' + f' AND CAST(SUBSTRING(number,{7-len_num+1},7) AS integer) between {int(numbers_len[0])} and {int(numbers_len[1])}')

            if id_test == 'all_numbers': 
                q['query'] = q['query'].replace(' AND number in {number_list}', '')

    return j_queries

      
def get_general_top(query, date_from, date_to):

    return select_from_bd(
        query.format(
            date_1=date_from.strftime('%Y-%m-%d'), 
            date_2=date_to.strftime('%Y-%m-%d')
        ), 
        source='CH'
    )

# def calculation_metrics(id_test, test_file, days_, j_queries, numbers_path=None):

#     for 
#     get_metrics(id_test, test_file, days_, j_queries, numbers_path=None)


def get_metrics(id_test, test_file, days_, j_queries, numbers_path=None):

    if id_test != 'all_numbers':
        numbers_list = get_numbers_type(test_file)

    number_list_path = get_numbers_path(numbers_path)

    step = 10000

    for number, line_metric in enumerate([q for q in j_queries if len(q['querys']) == 1]):
        metric = line_metric['metrics'][0]

        if metric.find('Маржа') > -1:
            continue

        instruc = line_metric['querys'][0]
        source = instruc['source']
        type_q = instruc['type']
        query_main = instruc['query'].strip()
        for date_from, date_to in days_:
            if id_test != 'all_numbers':
                for number_list, test_type in numbers_list:
                    if number_list:
                        file_tests = os.path.join(f'{os.path.dirname(__file__)}/all_answers/{id_test}{number_list_path}', f"{date_from.strftime('%Y%m%d')}_{date_to.strftime('%Y%m%d')}_{test_type}_{metric}.parquet")
                        if not os.path.exists(file_tests):
                            df_new = pd.DataFrame()
                            for page in range(0,len(number_list),step):
                                page_query = number_list[page:page+step]
                                if query_main != '':
                                    if source == 'MS':
                                        query = query_main.format(
                                                date_1=date_from.strftime('%Y%m%d'), 
                                                date_2=date_to.strftime('%Y%m%d'), 
                                                number_list=str(tuple(page_query)).replace(",)",")")
                                            )
                                    else:
                                        query = query_main.format(
                                                date_1=date_from.strftime('%Y-%m-%d'), 
                                                date_2=date_to.strftime('%Y-%m-%d'), 
                                                number_list=str(tuple(page_query)).replace(",)",")")
                                            )

                                    df_c = select_from_bd(query, source)
                                    if not df_c.empty:
                                        df_new = pd.concat([df_new,df_c])
                            
                            if not df_new.empty:
                                df_new['date'] = date_from
                                df_new['date_view'] = str(date_from)[:10]
                                df_new['metric'] = metric
                                df_new['type_q'] = type_q
                                df_new['test_type'] = test_type
                                df_new.to_parquet(file_tests)
                                os.chmod(file_tests, 0o777)

            else:
                file_tests = os.path.join(f'{os.path.dirname(__file__)}/all_answers/{id_test}{number_list_path}', f"{date_from.strftime('%Y%m%d')}_{date_to.strftime('%Y%m%d')}_{metric}.parquet")
                if not os.path.exists(file_tests):
                    df_new = pd.DataFrame()
                    
                    if query_main != '':
                        if source == 'MS':
                            query = query_main.format(
                                    date_1=date_from.strftime('%Y%m%d'), 
                                    date_2=date_to.strftime('%Y%m%d')
                                )
                        else:
                            query = query_main.format(
                                    date_1=date_from.strftime('%Y-%m-%d'), 
                                    date_2=date_to.strftime('%Y-%m-%d')
                                )

                    print(query)
                    
                    df_new = select_from_bd(query, source)
                    # if not df_c.empty:
                    #     df_new = pd.concat([df_new,df_c])
                    
                    if not df_new.empty:
                        df_new['date'] = date_from
                        df_new['date_view'] = str(date_from)[:10]
                        df_new['metric'] = metric
                        df_new['type_q'] = type_q
                        df_new['test_type'] = id_test
                        df_new.to_parquet(file_tests)
                        os.chmod(file_tests, 0o777)

        print(metric)

def get_metrics_complex(id_test, test_file, days_, j_queries, numbers_path=None):
    
    if id_test != 'all_numbers':
        numbers_list = get_numbers_type(test_file)

    number_list_path = get_numbers_path(numbers_path)

    for line_metric in [q for q in j_queries if len(q['querys']) > 1]:
        metric = line_metric['metrics'][0]

        if metric.find('Маржа') > -1:
            continue

        query_list = line_metric['querys']
        for date_from, date_to in days_:
            for query in query_list:
                source = query['source']
                action = query['type']
                query_main = query['query']

                if action == 'select':    
                    if id_test == 'all_numbers':
                        df_new = pd.DataFrame()
                        file_tests = os.path.join(f'{os.path.dirname(__file__)}/all_answers/{id_test}{number_list_path}', f"{date_from.strftime('%Y%m%d')}_{date_to.strftime('%Y%m%d')}_{metric}.parquet")
                        if not os.path.exists(file_tests):
                            if source == 'MS':
                                query = query_main.format(
                                        date_1=date_from.strftime('%Y%m%d'), 
                                        date_2=date_to.strftime('%Y%m%d')
                                    )
                            else:
                                query = query_main.format(
                                        date_1=date_from.strftime('%Y-%m-%d'), 
                                        date_2=date_to.strftime('%Y-%m-%d')
                                    )

                            df_new = select_from_bd(query, source)

                            if not df_new.empty:
                                df_new['test_type'] = id_test
                                df_new.to_parquet(file_tests)
                            
                    else:
                        for number_list, test_type in numbers_list:
                            if number_list:
                                df_new = pd.DataFrame()
                                file_tests = os.path.join(f'{os.path.dirname(__file__)}/all_answers/{id_test}{number_list_path}', f"{date_from.strftime('%Y%m%d')}_{date_to.strftime('%Y%m%d')}_{test_type}_{metric}.parquet")
                                if not os.path.exists(file_tests):
                                    step = 10000
                                    for page in range(0,len(number_list),step):
                                        page_query = number_list[page:page+step]

                                        if source == 'MS':
                                            query = query_main.format(
                                                    date_1=date_from.strftime('%Y%m%d'), 
                                                    date_2=date_to.strftime('%Y%m%d'),
                                                    number_list=str(tuple(page_query)).replace(",)",")")
                                                )
                                        else:
                                            query = query_main.format(
                                                    date_1=date_from.strftime('%Y-%m-%d'), 
                                                    date_2=date_to.strftime('%Y-%m-%d'),
                                                    number_list=str(tuple(page_query)).replace(",)",")")
                                                )
                                    
                                        df_c = select_from_bd(query, source=source)
                                        if not df_c.empty:
                                            df_new = pd.concat([df_new,df_c])

                                    if not df_new.empty:
                                        df_new['test_type'] = test_type
                                        df_new.to_parquet(file_tests)

                elif action == 'sum':
                    files = glob.glob(f"{os.path.dirname(__file__)}/all_answers/{id_test}{number_list_path}/{date_from.strftime('%Y%m%d')}_{date_to.strftime('%Y%m%d')}*{metric}.parquet")
                    for file in files:
                        df_new = pd.read_parquet(file)

                        if 'id_tov' not in df_new.columns:
                            continue

                        step = 5000
                        df_sum = pd.DataFrame()
                        for page in range(0,len(df_new),step):
                            kwargs = df_new[page:page+step].to_dict(orient='list')
                            id_tov = str(tuple([l for l in set(kwargs['id_tov']) if l])).replace(",)",")")
                            id_cart = str(tuple([l for l in set(kwargs['id_cart']) if l])).replace(",)",")")
                            if source == 'MS':
                                query = query_main.format(
                                        date_1=date_from.strftime('%Y%m%d'), 
                                        date_2=date_to.strftime('%Y%m%d'),
                                        id_tov=id_tov,
                                        id_cart=id_cart
                                    )
                            else:
                                query = query_main.format(
                                        date_1=date_from.strftime('%Y-%m-%d'), 
                                        date_2=date_to.strftime('%Y-%m-%d'),
                                        id_tov=id_tov,
                                        id_cart=id_cart
                                    )
                            
                            print(page)
                                    
                            df_c = select_from_bd(query, source=source)
                            if not df_c.empty:
                                df_sum = pd.concat([df_sum,df_c])
                            
                        if not df_sum.empty:
                            df_sum['date'] = date_from
                            df_sum['date_view'] = str(date_from)[:10]
                            df_sum['metric'] = metric
                            df_sum['type_q'] = action
                            df_sum['test_type'] = df_new['test_type'].unique()[0]
                            df_sum.to_parquet(file)
                            os.chmod(file, 0o777)


def get_top_query(date_from, date_to, number_len):
    file_ = os.path.join(f'{os.path.dirname(__file__)}/queries', 'top_query.sql')
    with open(file_) as f:
        query_main = f.read()

    df_all = pd.DataFrame()

    query = "SELECT search_bar, COUNT(DISTINCT number) as number_all_ FROM logs.tovs_search WHERE date_add between '{date_1}' and '{date_2}' GROUP BY search_bar"

    df_number_all = get_general_top(query, date_from, date_to)

    query = "SELECT search_bar, COUNT(id_search) as search_all_ FROM logs.tovs_search WHERE date_add between '{date_1}' and '{date_2}' GROUP BY search_bar"

    df_search_all = get_general_top(query, date_from, date_to)

    query = "SELECT search_bar, COUNT(id_search) as add_all FROM logs.tovs_search WHERE date_add between '{date_1}' and '{date_2}' AND id_element in ('add', 'button') GROUP BY search_bar"

    df_search_add_all = get_general_top(query, date_from, date_to)

    df_all = pd.concat([df_all, df_number_all])

    df_all = (
        df_all.merge(
            df_search_all,
            how='outer',
            on=['search_bar'],
            suffixes=['_x', '_y']
        )
    )

    df_all = (
        df_all.merge(
            df_search_add_all,
            how='outer',
            on=['search_bar'],
            suffixes=['_x', '_y']
        )
    )

    query_main = query_main.format(
        date_1=date_from.strftime('%Y-%m-%d'), 
        date_2=date_to.strftime('%Y-%m-%d')
    )

    num = 0

    for num_1, num_2 in number_len:
        len_num = len(num_1)

        query = "SELECT search_bar, COUNT(1) as search_bar_add_all_ FROM logs.tovs_search WHERE date_add between '{date_1}' and '{date_2}' AND id_element in ('add', 'button') AND number in number_list GROUP BY search_bar"
        query = query.replace('AND number in number_list', f' AND toInt32OrZero(substring(number,{7-len_num+1},7)) between {int(num_1)} and {int(num_2)}')

        df_search_bar_add_all = get_general_top(query, date_from, date_to)
        query = query_main.replace('AND number in number_list', f' AND toInt32OrZero(substring(number,{7-len_num+1},7)) between {int(num_1)} and {int(num_2)}')

        df_new = select_from_bd(query, source='CH')

        df_new = (
            df_new.merge(
                df_search_bar_add_all,
                how='outer',
                on=['search_bar'],
                suffixes=['_x', '_y']
            )
        )

        columns_int = ['cout_numbers','cout_id_search','search_bar_add_all_']
        columns_float = ['avg_position','avg_rn_max']

        df_new[columns_int] = df_new[columns_int].fillna(0).astype('int64').round(2)
        df_new[columns_float] = df_new[columns_float].fillna(0).astype('float64').round(2)

        df_new['konv_'] = df_new['search_bar_add_all_'] / df_new['cout_id_search']

        df_all = (
            df_all.merge(
                df_new,
                how='outer',
                on=['search_bar'],
                suffixes=['_x', '_y']
            )
        )

    if not df_all.empty: 
        df_all['delta_konv'] = df_all['konv__x'] / df_all['konv__y'] - 1
        df_all['delta_avg'] = df_all['avg_position_y'] / df_all['avg_position_x'] - 1 

        df_all = df_all.fillna(0).round(2)

        # df_all = df_all[(df_all['delta_konv'] > 0) | (df_all['delta_avg'] > 0)]

    return df_all.to_json(orient='records')
