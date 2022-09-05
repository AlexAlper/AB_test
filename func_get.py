
import json
import os
import glob
from datetime import timedelta, datetime
import body
import time
import pandas as pd
import pathlib
from pathlib import Path

DATE_TO_DATE = datetime.now()

def check_dir(path_name):
    path_test = os.path.join(f'{os.path.dirname(__file__)}/all_answers', path_name)
    if not os.path.exists(path_test):
        pathlib.Path(path_test).mkdir(parents=True, exist_ok=True)
        os.chmod(path_test, 0o777)

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

def get_info(id_test, date_begin, date_end):
    id_test = id_test.split(' ')[0]

    check_dir(id_test)

    days_ = body.get_time_on_days(date_begin, date_end + timedelta(days=1))

    if id_test == 'all_numbers':
        body.get_metrics_all(
            days_=days_
        )
    else:
        test_number = os.path.join(f'{os.path.dirname(__file__)}/all_answers/{id_test}', 'number.parquet')
        if os.path.exists(test_number):
            c_time = os.path.getctime(test_number)
            local_time = datetime.fromtimestamp(c_time)
            if local_time < DATE_TO_DATE - timedelta(days=1):
                body.get_all_numbers_test(
                    id_test=id_test,
                    test_file = test_number
                )
            else:
                print('file is fresh')
        else:
            body.get_all_numbers_test(
                id_test=id_test,
                test_file = test_number
            )

        body.get_metrics(
            id_test=id_test,
            test_file=test_number,
            days_=days_
        )
    
    df_full = pd.DataFrame()
    for date_from, date_to in days_:
        files = glob.glob(f"{os.path.dirname(__file__)}/all_answers/{id_test}/{date_from.strftime('%Y%m%d')}_{date_to.strftime('%Y%m%d')}*.parquet")
        for file in files:    
            df_new = pd.read_parquet(file)
            df_full = pd.concat([df_full,df_new])

    df_full_avg = pd.DataFrame()
    df_full_unique = pd.DataFrame()
    df_full_sum = pd.DataFrame()
    df_full_count = pd.DataFrame()
    if not df_full.empty:
        if id_test == 'all_numbers':
            df_full['date'] = df_full['date'].apply(lambda x: str(x)[:10])
            df_full.set_index(['metric', 'date'], inplace=True)
        else:
            df_full.set_index(['metric', 'test_type'], inplace=True)
        indexs = df_full.index.names
        # print(indexs)
        df_full_unique = df_full.loc[df_full.type_q == 'unique'].drop_duplicates()
        # print(df_full_unique)
        if not df_full_unique.empty:
            df_full_unique = df_full_unique['value'].groupby(level = df_full.index.names).count()
            print(df_full_unique)
        
        df_full_avg = df_full.loc[df_full.type_q == 'avg']
        if not df_full_avg.empty:
            df_full_avg['value'] = df_full_avg['value'].fillna('0').astype('int')
            df_full_avg = df_full_avg['value'].groupby(level = df_full.index.names).mean()
            print(df_full_avg)
        
        df_full_sum = df_full.loc[df_full.type_q == 'sum']
        if not df_full_sum.empty:
            df_full_sum['value'] = df_full_sum['value'].fillna('0').astype('int')
            df_full_sum = df_full_sum['value'].groupby(level = df_full.index.names).sum()
            print(df_full_sum)
        
        df_full_count = df_full.loc[df_full.type_q == 'count']
        if not df_full_count.empty:
            df_full_count['value'] = df_full_count['value'].fillna('0').astype('int')
            df_full_count = df_full_count['value'].groupby(level = df_full.index.names).sum()
            print(df_full_count)
        df_all = pd.concat([df_full_unique, df_full_avg, df_full_sum, df_full_count])
        # # for i, row in df_full_unique.iterrows():
        # #     print(list(row))
        print(df_all)
        # type_q = df_new['type_q'].unique()[0]
        # if type_q == 'unique':  
        #     df_new = df_new.drop_duplicates()
        #     df_new = df_new.count()
        # elif type_q == 'avg':
        #     df_new = df_new.fillna('0').astype('int')
        #     df_new = df_new['value'].mean()
        # elif type_q == 'count' or type_q == 'sum':  
        #     df_new = df_new.fillna('0').astype('int')
        #     answer = float(df_new['value'].sum())


    # df_full = df_full.groupby(level = df_full.index.names).sum()
    # df_full = df_full.reset_index()
    # df_full.rename(
    #     columns={
    #         'index': 'metrics'
    #     },
    #     inplace=True
    # )

    # print(df_full)
    # df_full['date'] = df_full['date'].astype('object')
    # print(df_full)

    # return df_full.to_json(orient='records')

    

if __name__=='__main__':
    print(datetime.now())
    date_begin = datetime(2022,8,29)
    date_end = datetime(2022,8,30)
    days_ = body.get_time_on_days(date_begin, date_end + timedelta(days=1))
    id_test=4
    test_number = os.path.join(f'{os.path.dirname(__file__)}/all_answers/{id_test}', 'number.parquet')
    get_tests()
    print(get_info("4 adsasd",date_begin, date_end))
    # body.get_metrics_complex(id_test=id_test,test_file=test_number,days_=days_)
    # print(get_info("all_numbers",date_begin, date_end))
    # check_dir('all_numbers')
    # body.get_metrics_all(date_begin, date_end)
    print(datetime.now())
