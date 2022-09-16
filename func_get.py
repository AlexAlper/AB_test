
import json
import os
import glob
from datetime import timedelta, datetime
import body
import time
import pandas as pd
import pathlib
from pathlib import Path
from metrica import Metrics

DATE_TO_DATE = datetime.now()

def check_dir(path_name):
    path_test = os.path.join(f'{os.path.dirname(__file__)}/all_answers', path_name)
    if not os.path.exists(path_test):
        pathlib.Path(path_test).mkdir(parents=True, exist_ok=True)
        os.chmod(path_test, 0o777)

    return path_test

def get_tests():
    file_tests = os.path.join(f'{os.path.dirname(__file__)}/all_answers', 'all_tests.json')
    if os.path.exists(file_tests):
        c_time = os.path.getctime(file_tests)
        local_time = datetime.fromtimestamp(c_time)
        if local_time < DATE_TO_DATE - timedelta(days=1):
            body.get_all_tests(file=file_tests)
    else:
        body.get_all_tests(file=file_tests)

    with open(file_tests) as f:
        all_tests = json.load(f)

    return [f"{i['id_ab_test']} {i['Descr_ab_test'].strip()}" for i in all_tests]

def get_info_2(id_test, date_begin, date_end, week=False, number_list=None, breakdown_by_dates=False):
    id_test = id_test.split(' ')[0]

    number_list_path = body.get_numbers_path(number_list)

    check_dir(f'{id_test}{number_list_path}')

    days_ = body.get_time_on_days(date_begin, date_end + timedelta(days=1))

    if id_test != 'all_numbers':
        test_number = os.path.join(f'{os.path.dirname(__file__)}/all_answers/{id_test}{number_list_path}', 'number.parquet')
        if os.path.exists(test_number):
            c_time = os.path.getctime(test_number)
            local_time = datetime.fromtimestamp(c_time)
            if local_time < DATE_TO_DATE - timedelta(days=1):
                body.get_all_numbers_test(
                    id_test=id_test,
                    test_file = test_number,
                    numbers_len=number_list
                )
            else:
                print(f'{test_number}: file is fresh')
        else:
            body.get_all_numbers_test(
                id_test=id_test,
                test_file = test_number,
                numbers_len=number_list
            )
    else:
        test_number = None

    j_queries = body.replase_querys(id_test,number_list)

    body.get_metrics(
        id_test=id_test,
        test_file=test_number,
        days_=days_,
        j_queries=j_queries,
        numbers_path=number_list
    )

    body.get_metrics_complex(
        id_test=id_test,
        test_file=test_number,
        days_=days_,
        j_queries=j_queries,
        numbers_path=number_list
    )

    print(1, datetime.now())

    df_full_avg = pd.DataFrame()
    df_full_unique = pd.DataFrame()
    df_full_sum = pd.DataFrame()
    df_full_count = pd.DataFrame()
    
    df_full = pd.DataFrame()
    for date_from, date_to in days_:
        files = glob.glob(f"{os.path.dirname(__file__)}/all_answers/{id_test}{number_list_path}/{date_from.strftime('%Y%m%d')}_{date_to.strftime('%Y%m%d')}*.parquet")
        for file in files:    
            df_new = pd.read_parquet(file)

            if 'unique' == df_new['type_q'].unique()[0]:
                df_full_unique = pd.concat([df_full_unique,df_new])

            if 'avg' == df_new['type_q'].unique()[0]:
                df_full_avg = pd.concat([df_full_avg,df_new])

            if 'sum' == df_new['type_q'].unique()[0]:
                df_full_sum = pd.concat([df_full_sum,df_new])

            if 'count' == df_new['type_q'].unique()[0]:
                df_full_count = pd.concat([df_full_count,df_new])

            # df_full = pd.concat([df_full,df_new])

    print(2, datetime.now())

    
    if id_test == 'all_numbers':
        columns = ['metric']

        # df_full_unique['date_view'] = df_full_unique['date'].apply(lambda x: str(x)[:10])
        # df_full_avg['date_view'] = df_full_avg['date'].apply(lambda x: str(x)[:10])
        # df_full_sum['date_view'] = df_full_sum['date'].apply(lambda x: str(x)[:10])
        # df_full_count['date_view'] = df_full_count['date'].apply(lambda x: str(x)[:10])
    else:
        columns = ['metric', 'test_type']

    if breakdown_by_dates:
        columns.append('date')
    
    if week:
        columns.append('week')

        if not df_full_unique.empty:
            df_full_unique['week'] = df_full_unique['date'].dt.week

        if not df_full_avg.empty:
            df_full_avg['week'] = df_full_avg['date'].dt.week

        if not df_full_sum.empty:
            df_full_sum['week'] = df_full_sum['date'].dt.week
        
        if not df_full_count.empty:
            df_full_count['week'] = df_full_count['date'].dt.week


    if not df_full_unique.empty:
        df_full_unique.set_index(columns, inplace=True)

    if not df_full_avg.empty:
        df_full_avg.set_index(columns, inplace=True)

    if not df_full_sum.empty:
        df_full_sum.set_index(columns, inplace=True)
    
    if not df_full_count.empty:
        df_full_count.set_index(columns, inplace=True)    


    print(3, datetime.now())
    
    df_all = pd.DataFrame()
    if not df_full_unique.empty:
        df_full_unique = df_full_unique['value'].drop_duplicates().groupby(level = columns).count()
        df_full_unique = df_full_unique.reset_index(level=columns)
        # print(df_full_unique)
        df_all = pd.concat([df_all, df_full_unique])
        print(4, datetime.now())
        
    if not df_full_avg.empty:
        df_full_avg['value'] = df_full_avg['value'].fillna('0').astype('int64')
        df_full_avg = df_full_avg['value'].groupby(level = columns).mean().round(2)
        df_full_avg = df_full_avg.reset_index(level=columns)
        # print(df_full_avg)
        df_all = pd.concat([df_all, df_full_avg])
        print(5, datetime.now())
        
    if not df_full_sum.empty:
        df_full_sum['value'] = df_full_sum['value'].fillna('0').astype('int64')
        df_full_sum = df_full_sum['value'].groupby(level = columns).sum()
        df_full_sum = df_full_sum.reset_index(level=columns)
        # print(df_full_sum)
        df_all = pd.concat([df_all, df_full_sum])
        print(6, datetime.now())
        
    if not df_full_count.empty:
        df_full_count['value'] = df_full_count['value'].fillna('0').astype('int64')
        df_full_count = df_full_count['value'].groupby(level = columns).sum()
        df_full_count = df_full_count.reset_index(level=columns)
        # print(df_full_count)
        df_all = pd.concat([df_all, df_full_count])
        print(7, datetime.now())
        
    print(99, datetime.now())

    # if id_test == 'all_numbers':
    #     df_all['date'] = df_all['date'].apply(lambda x: str(x)[:10])

    # df_all[df_all['metric']=='Количество сессий без добавления'] = df_all[df_all['metric']=="Количество поисковых сессий (Уточнение запроса или исправления ошибки - единая поисковая сессия)"] - df_all[df_all['metric']=='Количество сессий без добавления']

    return df_all.to_json(orient='records')

def get_info(id_test, date_begin, date_end, week=False, number_list=None, breakdown_by_dates=False):

    if number_list:
        i=0
        for num_ in number_list:
            num_1, num_2 = num_
            metrics = Metrics(
                id_test=id_test, 
                date_from=date_begin, 
                date_to=date_end,
                week=week,
                numbers_range=num_,
                breakdown_by_dates=breakdown_by_dates
            )
            metrics.add_unique_numbers_all()
            metrics.add_unique_numbers_bye_orders()
            metrics.add_unique_basket()
            metrics.add_count_tov_add()
            metrics.add_sum_bye_tov()
            # metrics.add_sum_margin_tov()
            metrics.add_unique_numbers_visiting_search()
            metrics.add_unique_numbers_unempty_q()
            metrics.add_unique_search_unempty_q()
            metrics.add_unique_search()
            metrics.add_count_tov_unempty_q()
            # # metrics.add_sum_tov_add_unempty_q()
            # # metrics.add_sum_margin_tov_add_unempty_q()
            metrics.add_count_search_add_empty_q()
            metrics.add_count_tov_add_empty_q()
            metrics.add_unique_search_not_add()
            metrics.add_count_search_not_rn()
            metrics.add_unique_q_not_rn()

            # # прверить
            metrics.add_count_search_with_interesting()
            # metrics.add_avg_position_add()
            metrics.add_count_add_position_6()
            metrics.add_count_add_position_12()
            metrics.add_count_add_position_40()
            # metrics.add_count_q_position_6()
            # metrics.add_count_q_position_12()
            # metrics.add_count_q_position_40()
            metrics.add_unique_search_position_6()
            metrics.add_unique_search_position_12()
            metrics.add_unique_search_position_40()
            metrics.add_avg_time_add()
            # # metrics.add_unique_session()
            # # metrics.add_unique_session_not_add()

            index = metrics.index

            if i==0:
                i=1
                index_all = metrics.index

                df_all = metrics.df_full
                df_all[f'{num_1}_{num_2}'] = df_all.drop(columns=index).sum(axis=1)
                index_all.append(f'{num_1}_{num_2}')
                df_all = df_all.drop(columns=df_all.drop(columns=index_all).columns)
            else:
                df_new = metrics.df_full
                df_new[f'{num_1}_{num_2}'] = df_new.drop(columns=index).sum(axis=1)
                index_all.append(f'{num_1}_{num_2}')
                df_all = (
                    df_all.merge(
                        df_new,
                        how='outer',
                        on=index
                    )
                )
                df_all = df_all.drop(columns=df_all.drop(columns=index_all).columns)

    else:
        metrics = Metrics(
            id_test=id_test, 
            date_from=date_begin, 
            date_to=date_end,
            week=week,
            number_list=number_list,
            breakdown_by_dates=breakdown_by_dates
        )
        metrics.add_unique_numbers_all()
        metrics.add_unique_numbers_bye_orders()
        metrics.add_unique_basket()
        metrics.add_count_tov_add()
        metrics.add_sum_bye_tov()
        # metrics.add_sum_margin_tov()
        metrics.add_unique_numbers_visiting_search()
        metrics.add_unique_numbers_unempty_q()
        metrics.add_unique_search_unempty_q()
        metrics.add_unique_search()
        metrics.add_count_tov_unempty_q()
        # # metrics.add_sum_tov_add_unempty_q()
        # # metrics.add_sum_margin_tov_add_unempty_q()
        metrics.add_count_search_add_empty_q()
        metrics.add_count_tov_add_empty_q()
        metrics.add_unique_search_not_add()
        metrics.add_count_search_not_rn()
        metrics.add_unique_q_not_rn()

        # # прверить
        metrics.add_count_search_with_interesting()
        # # metrics.add_avg_position_add()
        metrics.add_count_add_position_6()
        metrics.add_count_add_position_12()
        metrics.add_count_add_position_40()
        # metrics.add_count_q_position_6()
        # metrics.add_count_q_position_12()
        # metrics.add_count_q_position_40()
        metrics.add_unique_search_position_6()
        metrics.add_unique_search_position_12()
        metrics.add_unique_search_position_40()
        metrics.add_avg_time_add()
        # # metrics.add_unique_session()
        # # metrics.add_unique_session_not_add()

        index = metrics.index

        df_all = metrics.df_full
        df_all['value'] = df_all.drop(columns=index).sum(axis=1)
        index.append('value')
        df_all = df_all.drop(columns=df_all.drop(columns=index).columns)

    return df_all.to_json(orient='records')


def get_top(date_begin,date_end,numbers_len):

    date_end = date_end + timedelta(days=1)

    return body.get_top_query(date_begin,date_end,numbers_len)
    

if __name__=='__main__':
    print(datetime.now())
    # date_begin = datetime(2022,8,29)
    # date_end = datetime(2022,9,4)
    date_begin = datetime(2022,9,9)
    date_end = datetime(2022,9,10)
    days_ = body.get_time_on_days(date_begin, date_end + timedelta(days=1))


    # print(get_info(4, date_begin, date_end, week=True, number_list=[('0','4'), ('5','9')], breakdown_by_dates=False))
    # exit(0)

    # test_number = os.path.join(f'{os.path.dirname(__file__)}/all_answers/{id_test}', 'number.parquet')

    # get_tests()
    # print(get_info("4 adsasd",date_begin, date_end))
    # print(get_info("all_numbers",date_begin, date_end, week=True, number_list=['0','9']))
    # print(get_info("all_numbers",date_begin, date_end, week=True, number_list=['0','9'],breakdown_by_dates=True))
    # print(get_info("4 adsasd",date_begin, date_end, week=True, number_list=['1','5']))
    # print(get_info("all_numbers",date_begin, date_end))
    # print(get_info("all_numbers",date_begin, date_end, week=True, number_list=['1','5']))
    # print(get_top(date_begin,date_end,numbers_len=[('0','4'), ('5','9')]))
    # print(datetime.now())
    # for date_from, date_to in days_:
    metrics_1 = Metrics(
        id_test=4, 
        date_from=date_begin, 
        date_to=date_end
    )
    
    metrics_2 = Metrics(
        id_test=4, 
        numbers_range=('0','4'),
        date_from=date_begin, 
        date_to=date_end
    )

    metrics_3 = Metrics(
        id_test='all_numbers', 
        date_from=date_begin, 
        date_to=date_end
    )

    metrics_4 = Metrics(
        id_test='all_numbers', 
        numbers_range=('0','4'),
        date_from=date_begin, 
        date_to=date_end
    )

    metrics_1.add_unique_numbers_all()
    # metrics_2.add_unique_numbers_all()
    # metrics_3.add_unique_numbers_all()
    # metrics_4.add_unique_numbers_all()

    metrics_1.add_unique_numbers_bye_orders()
    # metrics_2.add_unique_numbers_bye_orders()
    # metrics_3.add_unique_numbers_bye_orders()
    # metrics_4.add_unique_numbers_bye_orders()

    metrics_1.add_unique_basket()
    # metrics_2.add_unique_basket()
    # metrics_3.add_unique_basket()
    # metrics_4.add_unique_basket()

    metrics_1.add_count_tov_add()
    # metrics_2.add_count_tov_add()
    # metrics_3.add_count_tov_add()
    # metrics_4.add_count_tov_add()

    metrics_1.add_sum_bye_tov()
    # metrics_2.add_sum_bye_tov()
    # metrics_3.add_sum_bye_tov()
    # metrics_4.add_sum_bye_tov()

    # metrics_1.add_sum_margin_tov()
    # metrics_2.add_sum_margin_tov()
    # metrics_3.add_sum_margin_tov()
    # metrics_4.add_sum_margin_tov()

    metrics_1.add_unique_numbers_visiting_search()
    # metrics_2.add_unique_numbers_visiting_search()
    # metrics_3.add_unique_numbers_visiting_search()
    # metrics_4.add_unique_numbers_visiting_search()

    metrics_1.add_unique_numbers_unempty_q()
    # metrics_2.add_unique_numbers_unempty_q()
    # metrics_3.add_unique_numbers_unempty_q()
    # metrics_4.add_unique_numbers_unempty_q()

    metrics_1.add_unique_search_unempty_q()
    # metrics_2.add_unique_search_unempty_q()
    # metrics_3.add_unique_search_unempty_q()
    # metrics_4.add_unique_search_unempty_q()

    metrics_1.add_unique_search()
    # metrics_2.add_unique_search()
    # metrics_3.add_unique_search()
    # metrics_4.add_unique_search()

    metrics_1.add_count_tov_unempty_q()
    # metrics_2.add_count_tov_unempty_q()
    # metrics_3.add_count_tov_unempty_q()
    # metrics_4.add_count_tov_unempty_q()

    # # metrics_1.add_sum_tov_add_unempty_q()
    # # metrics_2.add_sum_tov_add_unempty_q()
    # # metrics_3.add_sum_tov_add_unempty_q()
    # # metrics_4.add_sum_tov_add_unempty_q()

    # # metrics_1.add_sum_margin_tov_add_unempty_q()
    # # metrics_2.add_sum_margin_tov_add_unempty_q()
    # # metrics_3.add_sum_margin_tov_add_unempty_q()
    # # metrics_4.add_sum_margin_tov_add_unempty_q()

    metrics_1.add_count_search_add_empty_q()
    # metrics_2.add_count_search_add_empty_q()
    # metrics_3.add_count_search_add_empty_q()
    # metrics_4.add_count_search_add_empty_q()

    metrics_1.add_count_tov_add_empty_q()
    # metrics_2.add_count_tov_add_empty_q()
    # metrics_3.add_count_tov_add_empty_q()
    # metrics_4.add_count_tov_add_empty_q()

    metrics_1.add_unique_search_not_add()
    # metrics_2.add_unique_search_not_add()
    # metrics_3.add_unique_search_not_add()
    # metrics_4.add_unique_search_not_add()

    metrics_1.add_count_search_not_rn()
    # metrics_2.add_count_search_not_rn()
    # metrics_3.add_count_search_not_rn()
    # metrics_4.add_count_search_not_rn()

    metrics_1.add_unique_q_not_rn()
    # metrics_2.add_unique_q_not_rn()
    # metrics_3.add_unique_q_not_rn()
    # metrics_4.add_unique_q_not_rn()

    # # проверить
    metrics_1.add_count_search_with_interesting()
    # metrics_2.add_count_search_with_interesting()
    # metrics_3.add_count_search_with_interesting()
    # metrics_4.add_count_search_with_interesting()

    metrics_1.add_avg_position_add()
    # # metrics_2.add_avg_position_add()
    # # metrics_3.add_avg_position_add()
    # # metrics_4.add_avg_position_add()

    metrics_1.add_count_add_position_6()
    # metrics_2.add_count_add_position_6()
    # metrics_3.add_count_add_position_6()
    # metrics_4.add_count_add_position_6()

    metrics_1.add_count_add_position_12()
    # metrics_2.add_count_add_position_12()
    # metrics_3.add_count_add_position_12()
    # metrics_4.add_count_add_position_12()

    metrics_1.add_count_add_position_40()
    # metrics_2.add_count_add_position_40()
    # metrics_3.add_count_add_position_40()
    # metrics_4.add_count_add_position_40()

    metrics_1.add_count_q_position_6()
    # metrics_2.add_count_q_position_6()
    # metrics_3.add_count_q_position_6()
    # metrics_4.add_count_q_position_6()

    metrics_1.add_count_q_position_12()
    # metrics_2.add_count_q_position_12()
    # metrics_3.add_count_q_position_12()
    # metrics_4.add_count_q_position_12()

    metrics_1.add_count_q_position_40()
    # metrics_2.add_count_q_position_40()
    # metrics_3.add_count_q_position_40()
    # metrics_4.add_count_q_position_40()

    metrics_1.add_unique_search_position_6()
    # metrics_2.add_unique_search_position_6()
    # metrics_3.add_unique_search_position_6()
    # metrics_4.add_unique_search_position_6()

    metrics_1.add_unique_search_position_12()
    # metrics_2.add_unique_search_position_12()
    # metrics_3.add_unique_search_position_12()
    # metrics_4.add_unique_search_position_12()

    metrics_1.add_unique_search_position_40()
    # metrics_2.add_unique_search_position_40()
    # metrics_3.add_unique_search_position_40()
    # metrics_4.add_unique_search_position_40()

    metrics_1.add_avg_time_add()
    # metrics_2.add_avg_time_add()
    # metrics_3.add_avg_time_add()
    # metrics_4.add_avg_time_add()

    # # metrics_1.add_unique_session()
    # # metrics_2.add_unique_session()
    # # metrics_3.add_unique_session()
    # # metrics_4.add_unique_session()

    # # metrics_1.add_unique_session_not_add()
    # # metrics_2.add_unique_session_not_add()
    # # metrics_3.add_unique_session_not_add()
    # # metrics_4.add_unique_session_not_add()

    index = metrics_1.index

    df_1 = metrics_1.df_full
    df_1['value'] = df_1.drop(columns=index).sum(axis=1)
    index.append('value')
    df_1 = df_1.drop(columns=df_1.drop(columns=index).columns)

    print(1,df_1)
    # print(2,metrics_2.df_full)
    # print(3,metrics_3.df_full)
    # print(4,metrics_4.df_full)