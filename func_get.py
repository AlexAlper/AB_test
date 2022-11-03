
import json
import os
import glob
from datetime import timedelta, datetime
import top_query
import time
import pandas as pd
import pathlib
from pathlib import Path
from metrica import Metrics

DATE_TO_DATE = datetime.now()

def check_dir(path_name):
    path_test = os.path.join(f'{os.path.dirname(__file__)}/tmp_cache', path_name)
    if not os.path.exists(path_test):
        pathlib.Path(path_test).mkdir(parents=True, exist_ok=True)
        os.chmod(path_test, 0o777)

    return path_test

def get_tests():
    file_tests = os.path.join(f'{os.path.dirname(__file__)}/tmp_cache', 'all_tests.json')
    if os.path.exists(file_tests):
        c_time = os.path.getctime(file_tests)
        local_time = datetime.fromtimestamp(c_time)
        if local_time < DATE_TO_DATE - timedelta(days=1):
            top_query.get_all_tests(file=file_tests)
    else:
        top_query.get_all_tests(file=file_tests)

    with open(file_tests) as f:
        all_tests = json.load(f)

    return [f"{i['id_ab_test']} {i['Descr_ab_test'].strip()}" for i in all_tests]


def get_info(id_test, date_begin, date_end, week=False, number_list=None, breakdown_by_dates=False, delta_konv=0, delta_avg=0, df_numbers=pd.DataFrame):

    date_end = date_end + timedelta(days=1)

    if number_list:
        i=0
        list_unique_q_konversia_up = []
        list_add_unique_q_avg_down = []
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
            # metrics.add_unique_search_unempty_q()
            metrics.add_unique_search()
            metrics.add_count_tov_unempty_q()
            metrics.add_sum_tov_add_unempty_q()
            # metrics.add_sum_margin_tov_add_unempty_q()
            metrics.add_count_search_add_empty_q()
            metrics.add_count_tov_add_empty_q()
            metrics.add_unique_search_not_add()
            metrics.add_count_search_not_rn()
            metrics.add_unique_q_not_rn()
            metrics.add_count_search_with_interesting()
            metrics.add_avg_position_add()
            metrics.add_count_add_position_6()
            metrics.add_count_add_position_12()
            metrics.add_count_add_position_39()
            metrics.add_count_add_position_40()
            metrics.add_count_q_position_6()
            metrics.add_count_q_position_12()
            metrics.add_count_q_position_39()
            metrics.add_count_q_position_40()
            metrics.add_unique_search_position_6()
            metrics.add_unique_search_position_12()
            metrics.add_unique_search_position_39()
            metrics.add_unique_search_position_40()
            metrics.add_avg_time_add()
            # metrics.add_unique_session()
            # metrics.add_unique_session_not_add()
            metrics.add_unique_q_konversia_up()
            metrics.add_unique_q_avg_down()

            if delta_konv > 0:
                list_unique_q_konversia_up.append(metrics.add_unique_q_konversia_up())

            if delta_avg > 0:
                list_add_unique_q_avg_down.append(metrics.add_unique_q_avg_down())

            index = metrics.index

            if i==0:
                i=1
                index_all = metrics.index

                df_all = metrics.df_full
                df_all[f'{num_1}_{num_2}'] = df_all.drop(columns=index).sum(axis=1).astype('float64').round(2)
                index_all.append(f'{num_1}_{num_2}')
                df_all = df_all.drop(columns=df_all.drop(columns=index_all).columns)
            else:
                df_new = metrics.df_full
                df_new[f'{num_1}_{num_2}'] = df_new.drop(columns=index).sum(axis=1).astype('float64').round(2)
                index_all.append(f'{num_1}_{num_2}')
                df_all = (
                    df_all.merge(
                        df_new,
                        how='outer',
                        on=index
                    )
                )
                df_all = df_all.drop(columns=df_all.drop(columns=index_all).columns)

        if delta_konv > 0:
            df_unique_q_konversia_up = metrics.merge_unique_q_konversia_up(list_unique_q_konversia_up[0], list_unique_q_konversia_up[1], delta=delta_konv, number_list=number_list)
            df_all = pd.concat([df_all,df_unique_q_konversia_up])

        if delta_avg > 0:
            df_unique_q_avg_down = metrics.merge_unique_q_avg_down(list_add_unique_q_avg_down[0], list_add_unique_q_avg_down[1], delta=delta_avg, number_list=number_list)
            df_all = pd.concat([df_all,df_unique_q_avg_down])

    else:
        metrics = Metrics(
            id_test=id_test, 
            date_from=date_begin, 
            date_to=date_end,
            week=week,
            numbers_range=number_list,
            breakdown_by_dates=breakdown_by_dates
        )

        if not df_numbers.empty:
            metrics.create_numbers_group(df=df_numbers)
        
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
        metrics.add_count_search_with_interesting()
        metrics.add_avg_position_add()
        metrics.add_count_add_position_6()
        metrics.add_count_add_position_12()
        metrics.add_count_add_position_39()
        metrics.add_count_add_position_40()
        metrics.add_count_q_position_6()
        metrics.add_count_q_position_12()
        metrics.add_count_q_position_39()
        metrics.add_count_q_position_40()
        metrics.add_unique_search_position_6()
        metrics.add_unique_search_position_12()
        metrics.add_unique_search_position_39()
        metrics.add_unique_search_position_40()
        metrics.add_avg_time_add()
        # metrics.add_unique_session()
        # metrics.add_unique_session_not_add()
        metrics.add_unique_q_konversia_up()
        metrics.add_unique_q_avg_down()

        index = metrics.index

        df_all = metrics.df_full
        df_all['value'] = df_all.drop(columns=index).sum(axis=1).astype('float64').round(2)
        index.append('value')
        df_all = df_all.drop(columns=df_all.drop(columns=index).columns)

    df_all.to_csv('test.csv')

    print(df_all)

    return df_all.to_json(orient='records')


def get_top(date_begin,date_end,numbers_len, delta_plus=False):

    date_end = date_end + timedelta(days=1)

    df_all = top_query.get_top_query(date_begin,date_end,numbers_len, delta_plus)

    df_all.to_csv('test.csv')

    return df_all.to_json(orient='records')
    

if __name__=='__main__':
    print(datetime.now())
    # date_begin = datetime(2022,8,22)
    # date_end = datetime(2022,8,28)
    # date_begin = datetime(2022,8,2п9)
    # date_end = datetime(2022,9,43)
    # date_begin = datetime(2022,8,6)
    # date_end = datetime(2022,8,12)
    # date_begin = datetime(2022,9,9)
    # date_end = datetime(2022,9,10)
    # date_begin = datetime(2022,8,6)
    # date_end = datetime(2022,8,11)
    # date_begin = datetime(2022,9,15)
    # date_end = datetime(2022,9,22)
    date_begin = datetime(2022,9,12)
    date_end = datetime(2022,9,28)
    # df_numbers = pd.read_csv('test_search_fg_25.csv', sep=';')

    # print(get_info(4, date_begin, date_end, week=True, number_list=[('0','4'), ('5','9')], breakdown_by_dates=False))
    # print(get_info('all_numbers', date_begin, date_end, week=True, number_list=[('0','1'), ('5','6')], breakdown_by_dates=False))
    # print(get_info(4, date_begin, date_end))
    print(get_info('all_numbers', date_begin, date_end, number_list=[('0','4'), ('5','9')]))
    # print(get_info('1-25', date_begin, date_end, df_numbers=df_numbers))
    # print(get_info('all_numbers', date_begin, date_end, week=True, number_list=[('0','1'), ('5','6')], breakdown_by_dates=False, delta_konv=0.1, delta_avg=20))

    # print(get_top(date_begin,date_end,[('0','4'), ('5','9')], delta_plus=False))
    