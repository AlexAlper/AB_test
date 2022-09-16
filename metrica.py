from datetime import datetime, timedelta
from operator import index
import os
from select import select
import pandas as pd
from body import select_from_bd
from mssql import MsSQL
import pathlib
from dotenv import load_dotenv
from copy import deepcopy
import requests
import json
import glob

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

DATE_TO_DATE = datetime.now()

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

HEADER = {
    'X-ClickHouse-Format': 'JSONColumns'
}

sourсe_06 = MsSQL(
    params=deepcopy(ms_c_06)
)


class Metrics():

    def __init__(
            self,
            id_test: int,
            date_from: datetime,
            date_to: datetime,
            numbers_range=None,
            week=False,
            breakdown_by_dates=False,

    ):
        self.id_test = id_test
        self.numbers_range = numbers_range
        self.path_cache = self.check_dir()
        self.date_from = date_from
        self.date_to = date_to
        self.file_numbers = self.get_all_numbers_test()
        self.number_groups = self.get_groups_numbers()
        self.days_ = self.get_time_on_days()
        self.week = week
        self.breakdown_by_dates = breakdown_by_dates
        self.index = self.generate_index()
        self.df_full = pd.DataFrame()

    def generate_index(self):
        index = []

        if self.id_test == 'all_numbers':
            index.append('metric')
        else:
            index.append('metric')
            index.append('test_type')

        if self.week:
            index.append('week')

        if not self.week and self.breakdown_by_dates:
            index.append('date_view')

        return index

    def check_dir(self):
        path_name = f'{self.id_test}{self.get_numbers_path()}'
        path_test = os.path.join(f'{os.path.dirname(__file__)}/tmp_cache', path_name)
        if not os.path.exists(path_test):
            pathlib.Path(path_test).mkdir(parents=True, exist_ok=True)
            os.chmod(path_test, 0o777)

        return path_test

    def get_time_on_days(self):
        date_begin = self.date_from
        date_end = self.date_to
        days = []
        while date_begin < date_end:
            days.append((date_begin, date_begin + timedelta(days=1)))
            date_begin = date_begin + timedelta(days=1)

        return days

    def get_numbers_path(self):

        if self.numbers_range:
            num_1, num_2 = self.numbers_range
            number_list_path = f'/{num_1}_{num_2}'
        else:
            number_list_path = ''

        return number_list_path

    def get_all_numbers_test(self):

        if self.id_test == 'all_numbers':
            return None

        file_numbers = os.path.join(self.path_cache, 'number.parquet')
        if os.path.exists(file_numbers):
            c_time = os.path.getctime(file_numbers)
            local_time = datetime.fromtimestamp(c_time)
            if local_time < DATE_TO_DATE - timedelta(days=1):
                pass
            else:
                print(f'file is fresh: {file_numbers}')
                return file_numbers
        else:
            pass

        get_all_numbers = os.path.join('queries', 'get_all_numbers.sql')
        with open(get_all_numbers) as f:
            query = f.read()

        if self.numbers_range:
            num_1, num_2 = self.numbers_range
            len_num = len(num_1)
            query = query.replace(
                'id_ab_test = {id_test}',
                'id_ab_test = {id_test}' + f' AND CAST(SUBSTRING(number,{7 - len_num},7) AS integer) between {int(num_1)} and {int(num_2)}'
            )

        sourсe = MsSQL(
            params=deepcopy(ms_c_03)
        )

        df = sourсe.select_to_df(query.format(id_test=self.id_test))
        df['Type_test'] = df['Type_test'].str.strip()
        df.to_parquet(file_numbers)
        os.chmod(file_numbers, 0o777)

        return file_numbers

    def get_groups_numbers(self):

        if self.id_test == 'all_numbers':
            return None

        df_numbers = pd.read_parquet(self.file_numbers)
        df_numbers = df_numbers[['number', 'Type_test']]

        uniqu_type = df_numbers['Type_test'].unique()

        numbers_b = None
        if 'B' in uniqu_type:
            numbers_b = [i.upper().replace('\'', '') for i in
                         df_numbers[df_numbers['Type_test'] == 'B']['number'].unique()]

        numbers_a = [i.upper().replace('\'', '') for i in df_numbers[df_numbers['Type_test'] != 'B']['number'].unique()]

        return [(numbers_a, 'A'), (numbers_b, 'B')]

    def replase_querys_general(self, query: str, source: str, date_from, date_to):

        if self.numbers_range:
            num_1, num_2 = self.numbers_range
            len_num = len(num_1)
            if source == 'CH':
                query = query.replace(
                    'AND number in {number_list}',
                    'AND number in {number_list}' + f' AND toInt32OrZero(substring(number,{7 - len_num + 1},7)) between {int(num_1)} and {int(num_2)}'
                )
            if source == 'MS':
                query = query.replace(
                    'AND number in {number_list}',
                    'AND number in {number_list}' + f' AND CAST(SUBSTRING(number,{7 - len_num + 1},7) AS integer) between {int(num_1)} and {int(num_2)}'
                )

        if self.id_test == 'all_numbers':
            query = query.replace(' AND number in {number_list}', '')

        if source == 'MS':
            query = query.replace('{date_1}', date_from.strftime('%Y%m%d')).replace('{date_2}',
                                                                                    date_to.strftime('%Y%m%d'))
        else:
            query = query.replace('{date_1}', date_from.strftime('%Y-%m-%d')).replace('{date_2}',
                                                                                      date_to.strftime('%Y-%m-%d'))

        return query

    def _select_from_bd(self, query, source):
        df_new = pd.DataFrame()

        if source == 'MS':
            df_new = sourсe_06.select_to_df(query)
        else:
            http = requests.post(
                url=url,
                data=query,
                headers=HEADER
            )

            if http.status_code != 200:
                return df_new

            df_new = pd.DataFrame(json.loads(http.text))

        return df_new

    def get_metrics(self, query, name_param, source, type_q, metric, date_from, date_to, step=10000):

        print(metric)
        print(query)

        id_test = self.id_test

        if id_test != 'all_numbers':
            for number_list, test_type in self.number_groups:
                if number_list:
                    file_tests = os.path.join(
                        self.path_cache,
                        f"{date_from.strftime('%Y%m%d')}_{date_to.strftime('%Y%m%d')}_{test_type}_{name_param}.parquet"
                    )
                    if not os.path.exists(file_tests):
                        df_new = pd.DataFrame()
                        for page in range(0, len(number_list), step):
                            page_query = number_list[page:page + step]
                            query_q = query.format(
                                number_list=str(tuple(page_query)).replace(",)", ")")
                            )

                            if query != '':
                                df_c = self._select_from_bd(query_q, source)
                                if not df_c.empty:
                                    df_new = pd.concat([df_new, df_c])
                            else:
                                raise 'text query is empty'

                        if not df_new.empty:
                            df_new['date'] = date_from
                            df_new['date_view'] = str(date_from)[:10]
                            df_new['name_param'] = name_param
                            df_new['metric'] = metric
                            df_new['type_q'] = type_q
                            df_new['test_type'] = test_type
                            df_new['week'] = df_new['date'].dt.week
                            df_new.to_parquet(file_tests)
                            os.chmod(file_tests, 0o777)

        else:
            file_tests = os.path.join(
                self.path_cache,
                f"{date_from.strftime('%Y%m%d')}_{date_to.strftime('%Y%m%d')}_{name_param}.parquet"
            )
            if not os.path.exists(file_tests):
                df_new = pd.DataFrame()
                df_new = self._select_from_bd(query, source)

                if not df_new.empty:
                    df_new['date'] = date_from
                    df_new['date_view'] = str(date_from)[:10]
                    df_new['metric'] = metric
                    df_new['type_q'] = type_q
                    df_new['test_type'] = id_test
                    df_new['week'] = df_new['date'].dt.week
                    df_new.to_parquet(file_tests)
                    os.chmod(file_tests, 0o777)

    def get_df_full_for_metric(self, query, metric, name_param, source, type_q):
        for date_from, date_to in self.days_:
            self.get_metrics(
                query=self.replase_querys_general(query, source, date_from, date_to),
                date_from=date_from,
                date_to=date_to,
                name_param=name_param,
                source=source,
                type_q=type_q,
                metric=metric
            )

        df = self.merge_data(name_param)
        df.set_index(self.index, inplace=True)
        df = df.rename(
            columns={
                'value': name_param,
            }
        )

        if type_q == "unique":
            df = df[name_param].drop_duplicates().groupby(level=self.index).count()
        elif type_q == "count" or type_q == "sum":
            df = df[name_param].fillna('0').astype('int64').groupby(level=self.index).sum()
        elif type_q == "avg":
            df = df[name_param].fillna('0').astype('int64').groupby(level=self.index).mean().round(2)

        df = self.get_zero_values(df, name_param, metric)

        df = df.reset_index(level=self.index)
        
        return df

    def get_metrics_complex(self, query, name_param, source, type_q, metric, step=5000):

        pass

        # date_from = self.date_from
        # date_to = self.date_to
        # id_test = self.id_test

        # if type_q == 'sum':
        #     files = glob.glob(f"{self.path_cache}/{date_from.strftime('%Y%m%d')}_{date_to.strftime('%Y%m%d')}*{metric}.parquet")
        #     for file in files:
        #         df_new = pd.read_parquet(file)

        #         if 'id_tov' not in df_new.columns:
        #             continue

        #         step = 5000
        #         df_sum = pd.DataFrame()
        #         for page in range(0,len(df_new),step):
        #             kwargs = df_new[page:page+step].to_dict(orient='list')
        #             id_tov = str(tuple([l for l in set(kwargs['id_tov']) if l])).replace(",)",")")
        #             id_cart = str(tuple([l for l in set(kwargs['id_cart']) if l])).replace(",)",")")
        #             query = query.format(
        #                 id_tov=id_tov,
        #                 id_cart=id_cart
        #             )

        #             print(page)

        #             df_c = select_from_bd(query, source=source)
        #             if not df_c.empty:
        #                 df_sum = pd.concat([df_sum,df_c])

        #         if not df_sum.empty:
        #             df_sum['date'] = date_from
        #             df_sum['date_view'] = str(date_from)[:10]
        #             df_sum['metric'] = metric
        #             df_sum['type_q'] = action
        #             df_sum['test_type'] = df_new['test_type'].unique()[0]
        #             df_sum.to_parquet(file)
        #             os.chmod(file, 0o777)

    def merge_data(self,name_param):
        df = pd.DataFrame()
        for date_from, date_to in self.days_:
            files = glob.glob(f"{self.path_cache}/{date_from.strftime('%Y%m%d')}_{date_to.strftime('%Y%m%d')}*{name_param}.parquet")
            for file in files:
                df = pd.concat([df, pd.read_parquet(file)])

        return df

    def get_zero_values(self, df, name_param, metric):

        df = df.reset_index(level=self.index)

        if self.id_test != 'all_numbers':
            df_zero = pd.DataFrame()
            for t in ['A', 'B']:
                new_row = df.copy()[-1:]
                new_row['test_type'] = t
                new_row[name_param] = 0
                df_zero = pd.concat([df_zero, new_row])
        else:
            df_zero = pd.DataFrame(
                {
                    'metric': [metric],
                    name_param: [0]
                }
            )

        df = pd.concat([df, df_zero])

        df = df.groupby(by=self.index).sum()

        return df

    def add_unique_numbers_all(self):
        metric = "Количество клиентов, посещавших МП"
        name_param = 'unique_numbers_all'
        source = "CH"
        type_q = "unique"

        query = "SELECT DISTINCT (number) AS value " \
                "FROM logs.full_add_cart " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND source IN (2, 3, 4, 5)"
        
        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_unique_numbers_bye_orders(self):
        metric = "Количество клиентов, оформивших заказ"
        name_param = 'unique_numbers_bye_orders'
        source = "MS"
        type_q = "unique"

        query = "SELECT DISTINCT (number) as value " \
                "FROM Loyalty03..Orders_header WITH (NOLOCK) " \
                "WHERE date_order between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND source IN (2, 3, 4, 5) " \
                "AND id_status in (5, 9, 18) " \
                "AND order_type in (2, 4)"
        
        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_unique_basket(self):
        metric = "Количество уникальных корзин за период"
        name_param = 'unique_basket'
        source = "CH"
        type_q = "unique"

        query = "SELECT DISTINCT (id_cart) as value " \
                "FROM logs.full_add_cart " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list}"

        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_count_tov_add(self):
        metric = "Количество добавлений товаров из всех мест МП (поиск, каталог, подборки и др.)"
        name_param = 'count_tov_add'
        source = "CH"
        type_q = "count"

        query = "SELECT count(id_tov) as value " \
                "FROM logs.full_add_cart " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND isNotNull(id_tov) " \
                "AND id_element IN ('add', 'button')"

        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_sum_bye_tov(self):
        metric = "Выручка добавленных товаров из всех мест МП (поиск, каталог, подборки и др.)"
        name_param = 'sum_bye_tov'
        source = "MS"
        type_q = "sum"

        query = "SELECT sum(Order_line.base_sum) as value " \
                "FROM Loyalty03..Orders_header as Orders_header (NOLOCK) " \
                "INNER JOIN Loyalty03..Order_line AS Order_line (NOLOCK) " \
                "ON Order_line.id_order = Orders_header.id_order " \
                "WHERE Orders_header.date_order between '{date_1}' and '{date_2}' " \
                "AND source IN (2, 3, 4, 5) " \
                "AND id_status in (5, 9, 18) " \
                "AND order_type in (2, 4) " \
                "AND number in {number_list}"

        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_sum_margin_tov(self):
        metric = "Маржа добавленных товаров из всех мест МП (поиск, каталог, подборки и др.)"
        name_param = 'sum_margin_tov'
        source = "MS"
        type_q = "sum"

        query = "SELECT sum(Order_line.base_sum - Order_line.quantity * Sebest_tov_tbl.Sebestoimost_nds) as value " \
                "FROM Loyalty03..Orders_header as Orders_header (NOLOCK) " \
                "INNER JOIN Loyalty03..Order_line AS Order_line (NOLOCK) " \
                "ON Order_line.id_order = Orders_header.id_order " \
                "INNER JOIN NSI..Sebest_tov_tbl AS Sebest_tov_tbl (NOLOCK) " \
                "ON Sebest_tov_tbl.Tovar_Id = Order_line.id_tov " \
                "AND CAST(Sebest_tov_tbl._Period AS date) = " \
                "CAST(FORMAT(Orders_header.date_order, 'yyyy-MM-01') AS date) " \
                "WHERE Orders_header.date_order between '{date_1}' and '{date_2}' " \
                "AND source IN (2, 3, 4, 5) " \
                "AND id_status in (5, 9, 18) " \
                "AND order_type in (2, 4) " \
                "AND number in {number_list}"

        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_unique_numbers_visiting_search(self):
        metric = "Количество клиентов, заходивших в поиск"
        name_param = 'unique_numbers_visiting_search'
        source = "CH"
        type_q = "unique"

        query = "SELECT DISTINCT (number) as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' AND number in {number_list}"

        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_unique_numbers_unempty_q(self):
        metric = "Количество клиентов, добавлявших товары из непустого поиска"
        name_param = 'unique_numbers_unempty_q'
        source = "CH"
        type_q = "unique"

        query = "SELECT DISTINCT (number) as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND search_bar != '' " \
                "AND id_element IN ('add', 'button') " \
                "AND id_screen not like 'catalog/catalog_search/subcategories%'"

        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_unique_search_unempty_q(self):
        metric = "Количество уникальных выдач (не пустых)"
        name_param = 'unique_search_unempty_q'
        source = "CH"
        type_q = "count"

        query = "SELECT COUNT(DISTINCT id_search) as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND search_bar != ''"

        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_unique_search(self):
        metric = "Количество разных запросов"
        name_param = 'unique_search'
        source = "CH"
        type_q = "unique"

        query = "SELECT DISTINCT (search_bar) as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list}"

        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_unique_unempty_q(self):
        metric = "Количество уникальных выдач с добавлением (не пустых)"
        name_param = 'unique_unempty_q'
        source = "CH"
        type_q = "unique"

        query = "SELECT DISTINCT (id_search) as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND id_element in ('add', 'button') " \
                "AND search_bar != ''"

        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_count_tov_unempty_q(self):
        metric = "Количество товаров добавленных из поиска (не пустого)"
        name_param = 'count_tov_unempty_q'
        source = "CH"
        type_q = "count"

        query = "SELECT COUNT(id_tov) as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND isNotNull(id_tov) " \
                "AND id_screen not like 'catalog/catalog_search/subcategories%' " \
                "AND id_element in ('add', 'button') " \
                "AND search_bar != ''"

        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_sum_tov_add_unempty_q(self):
        metric = "Выручка товаров добавленных из поиска (не пустого)"
        name_param = 'sum_tov_add_unempty_q'
        source = "CH"
        type_q = "unique"

        query = "SELECT id_cart, id_tov " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND isNotNull(id_tov) " \
                "AND id_element in ('add', 'button') " \
                "AND search_bar != '' GROUP BY id_cart, id_tov"
        query = self.replase_querys_general(query, source)
        self.get_metrics(query, name_param, source, type_q, metric)

        source = "MS"
        type_q = "sum"

        query = "SELECT sum(Order_line.base_sum) as value " \
                "FROM Loyalty03..Orders_header as Orders_header " \
                "INNER JOIN Loyalty03..Order_line AS Order_line " \
                "ON Order_line.id_order = Orders_header.id_order " \
                "AND Order_line.id_tov in {id_tov} " \
                "WHERE Orders_header.date_order between '{date_1}' and '{date_2}' " \
                "AND source IN (2, 3, 4, 5) " \
                "AND id_status in (5, 9, 18) " \
                "AND order_type in (2, 4) " \
                "AND Orders_header.id_cart in {id_cart}"
        query = self.replase_querys_general(query, source)

    def add_sum_margin_tov_add_unempty_q(self):
        metric = "Маржа товаров добавленных из поиска (не пустого)"
        name_param = 'sum_tov_add_unempty_q'
        source = "CH"
        type_q = "select"

        query = "SELECT id_cart, id_tov " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND isNotNull(id_tov) " \
                "AND id_element in ('add', 'button') " \
                "AND search_bar != '' " \
                "GROUP BY id_cart, id_tov"
        query = self.replase_querys_general(query, source)
        self.get_metrics(query, name_param, source, type_q, metric)

        source = "MS"
        type_q = "sum"

        query = "SELECT sum(Order_line.base_sum) as value " \
                "FROM Loyalty03..Orders_header as Orders_header " \
                "INNER JOIN Loyalty03..Order_line AS Order_line " \
                "ON Order_line.id_order = Orders_header.id_order " \
                "AND Order_line.id_tov in {id_tov} " \
                "WHERE Orders_header.date_order between '{date_1}' and '{date_2}' " \
                "AND source IN (2, 3, 4, 5) " \
                "AND id_status in (5, 9, 18) " \
                "AND order_type in (2, 4) " \
                "AND Orders_header.id_cart in {id_cart}"
        query = self.replase_querys_general(query, source)

    def add_count_search_add_empty_q(self):
        metric = "Количество выдач пустого поиска (рекомендации в пустом поиске) с добавлением"
        name_param = 'count_search_add_unempty_q'
        source = "CH"
        type_q = "count"

        query = "SELECT COUNT(id_search) as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND search_bar = '' " \
                "AND id_screen not like 'catalog/catalog_search/subcategories%' " \
                "AND id_element in ('add', 'button')"

        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_count_tov_add_empty_q(self):
        metric = "Количество товаров, добавленных из пустого поиска"
        name_param = 'count_tov_add_empty_q'
        source = "CH"
        type_q = "count"

        query = "SELECT COUNT(id_tov) as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND isNotNull(id_tov) " \
                "AND number in {number_list} " \
                "AND id_screen not like 'catalog/catalog_search/subcategories%' " \
                "AND id_element in ('add', 'button') " \
                "AND search_bar = ''"

        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_unique_search_not_add(self):
        metric = "Количество разных запросов без добавления (ни одного добавления за период)"
        name_param = 'unique_search_not_add'
        source = "CH"
        type_q = "unique"

        query = "WITH ss as (" \
                "SELECT uniqExact(search_bar) as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list})SELECT (select * from ss) - uniqExact(search_bar) as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} AND id_element in ('add', 'button')"
        
        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_count_search_not_rn(self):
        metric = "Количество выдач 'Ничего не найдено'"
        name_param = 'count_search_not_rn'
        source = "CH"
        type_q = "unique"

        query = "SELECT DISTINCT (id_search) as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND rn_max = 0 " \
                "AND search_bar != ''"
        
        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_unique_q_not_rn(self):
        metric = "Количество запросов 'Ничего не найдено'"
        name_param = 'unique_q_not_rn'
        source = "CH"
        type_q = "unique"

        query = "SELECT DISTINCT (id_search) as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND rn_max = 0 " \
                "AND search_bar != ''"
        
        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_count_search_with_interesting(self):
        # это все действия с товаром пользователя
        metric = "Количество выдач с интересом: любое действие: добавление, " \
                 "просмотр карточки товара, привозите больше, избранное, списки"

        name_param = 'count_search_with_interesting'
        source = "CH"
        type_q = "count"

        query = "SELECT COUNT(id_search) as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND id_element not in ('delete', 'start', 'back', 'scroll_down')"

        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_avg_position_add(self):
        metric = "Средняя позиция добавления"
        name_param = 'avg_position_add'
        source = "CH"
        type_q = "avg"

        query = "SELECT position as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND position > 0 " \
                "AND id_element in ('add', 'button')"
       
        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_count_add_position_6(self):
        metric = "Количество добавлений с позицией добавления более 6"
        name_param = 'count_add_position_6'
        source = "CH"
        type_q = "count"

        query = "SELECT COUNT(id_search) as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND position > 6 " \
                "AND id_element in ('add', 'button')"

        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_count_add_position_12(self):
        metric = "Количество добавлений с позицией добавления более 12"
        name_param = 'count_add_position_12'
        source = "CH"
        type_q = "count"

        query = "SELECT COUNT(id_search) as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND position > 12 " \
                "AND id_element in ('add', 'button')"

        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_count_add_position_40(self):
        metric = "Количество добавлений с позицией добавления более 40"
        name_param = 'count_add_position_40'
        source = "CH"
        type_q = "count"

        query = "SELECT COUNT(id_search) as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND position > 40 " \
                "AND id_element in ('add', 'button')"

        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_count_q_position_6(self):
        metric = "Количество запросов со средней позицией добавления более 6"
        name_param = 'count_q_position_6'
        source = "CH"
        type_q = "unique"

        query = "SELECT search_bar, position " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND id_element in ('add', 'button') "

        for date_from, date_to in self.days_:
            self.get_metrics(
                query=self.replase_querys_general(query, source, date_from, date_to),
                date_from=date_from,
                date_to=date_to,
                name_param='count_q_position_all',
                source=source,
                type_q=type_q,
                metric=metric
            )

        df = self.merge_data('count_q_position_all')

        df_avg = df[self.index + ['search_bar','position']].groupby(self.index + ['search_bar']).mean().round(2)
        df_avg = df_avg[df_avg['position'] > 6]
        df_avg = df_avg.reset_index(self.index + ['search_bar']).drop(columns='position')
        df_avg = df_avg.rename(
            columns={
                'search_bar': name_param
            }
        )
        df_avg = df_avg.groupby(self.index).count()
        df_avg = df_avg.reset_index(self.index)
        self.df_full = pd.concat([self.df_full, df_avg])


    def add_count_q_position_12(self):
        metric = "Количество запросов со средней позицией добавления более 12"
        name_param = 'count_q_position_12'
        source = "CH"
        type_q = "unique"

        query = "SELECT COUNT(search_bar) as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND position > 12 " \
                "AND id_element in ('add', 'button')"

        for date_from, date_to in self.days_:
            self.get_metrics(
                query=self.replase_querys_general(query, source, date_from, date_to),
                date_from=date_from,
                date_to=date_to,
                name_param='count_q_position_all',
                source=source,
                type_q=type_q,
                metric=metric
            )

        df = self.merge_data('count_q_position_all')

        df_avg = df[self.index + ['search_bar','position']].groupby(self.index + ['search_bar']).mean().round(2)
        df_avg = df_avg[df_avg['position'] > 12]
        df_avg = df_avg.reset_index(self.index + ['search_bar']).drop(columns='position')
        df_avg = df_avg.rename(
            columns={
                'search_bar': name_param
            }
        )
        df_avg = df_avg.groupby(self.index).count()
        df_avg = df_avg.reset_index(self.index)
        self.df_full = pd.concat([self.df_full, df_avg])


    def add_count_q_position_40(self):
        metric = "Количество запросов со средней позицией добавления более 40"
        name_param = 'count_q_position_40'
        source = "CH"
        type_q = "unique"

        query = "SELECT COUNT(search_bar) as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND position > 40 " \
                "AND id_element in ('add', 'button')"

        for date_from, date_to in self.days_:
            self.get_metrics(
                query=self.replase_querys_general(query, source, date_from, date_to),
                date_from=date_from,
                date_to=date_to,
                name_param='count_q_position_all',
                source=source,
                type_q=type_q,
                metric=metric
            )

        df = self.merge_data('count_q_position_all')

        df_avg = df[self.index + ['search_bar','position']].groupby(self.index + ['search_bar']).mean().round(2)
        df_avg = df_avg[df_avg['position'] > 40]
        df_avg = df_avg.reset_index(self.index + ['search_bar']).drop(columns='position')
        df_avg = df_avg.rename(
            columns={
                'search_bar': name_param
            }
        )
        df_avg = df_avg.groupby(self.index).count()
        df_avg = df_avg.reset_index(self.index)
        self.df_full = pd.concat([self.df_full, df_avg])


    def add_unique_search_position_6(self):
        metric = "Количество уникальных выдач с позицией добавления более 6"
        name_param = 'unique_search_position_6'
        source = "CH"
        type_q = "unique"

        query = "SELECT DISTINCT (id_search) as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND position > 6 " \
                "AND id_element in ('add', 'button')"
        
        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_unique_search_position_12(self):
        metric = "Количество уникальных выдач с позицией добавления более 12"
        name_param = 'unique_search_position_12'
        source = "CH"
        type_q = "unique"

        query = "SELECT DISTINCT (id_search) as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND position > 12 " \
                "AND id_element in ('add', 'button')"
        
        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_unique_search_position_40(self):
        metric = "Количество уникальных выдач с позицией добавления более 40"
        name_param = 'unique_search_position_40'
        source = "CH"
        type_q = "unique"

        query = "SELECT DISTINCT (id_search) as value " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND position > 40 " \
                "AND id_element in ('add', 'button')"
        
        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    def add_avg_time_add(self):
        # проверить
        metric = "Среднее время от запроса до добавления"
        name_param = 'avg_time_add'
        source = "CH"
        type_q = "avg"

        query = "WITH min_search AS (" \
                "SELECT min(date_add) as min_d,  id_search as id_search " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND id_element in ('add', 'plus', 'minus', 'edit') " \
                "GROUP BY id_search" \
                "), min_add as (" \
                "SELECT min(date_add) as min_d, id_search as id_search " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND id_element in ('add', 'plus', 'minus', 'edit') " \
                "GROUP BY id_search, id_tov" \
                ") " \
                "SELECT date_diff('second', min_search.min_d, min_add.min_d) as value " \
                "FROM min_search INNER JOIN min_add USING(id_search)"
        
        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    # пока не считаем
    def add_unique_session(self):
        metric = "Количество поисковых сессий (Уточнение запроса или исправления ошибки - единая поисковая сессия)"
        name_param = 'unique_session'
        source = "CH"
        type_q = "unique"

        query = "SELECT number, id_search " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "GROUP BY number, id_search"
        
        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])

    # пока не считаем
    def add_unique_session_not_add(self):
        metric = "Количество сессий без добавления"
        name_param = 'unique_session_not_add'
        source = "CH"
        type_q = "unique"

        query = "SELECT number, id_search " \
                "FROM logs.tovs_search " \
                "WHERE date_add between '{date_1}' and '{date_2}' " \
                "AND number in {number_list} " \
                "AND id_element in ('add', 'button') " \
                "GROUP BY number, id_search"
        
        df = self.get_df_full_for_metric(query, metric, name_param, source, type_q)
        self.df_full = pd.concat([self.df_full, df])
