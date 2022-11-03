from calendar import month
import top_query
from datetime import datetime, timedelta
from bcp_load import DataManipulation
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from mssql import MsSQL
from copy import deepcopy
import requests
import json
from time import sleep


sourсe_06 = MsSQL(
            params=deepcopy(top_query.ms_c_06)
        )


con_str = f'postgresql://{top_query.PG_USER}:{top_query.PG_PASS}@{top_query.PG_HOST}/{top_query.PG_SCHEMA}'
engine = create_engine(con_str, poolclass=NullPool, isolation_level='AUTOCOMMIT')

url = f'http://{top_query.CH_USER}:{top_query.CH_PASS}@{top_query.CH_HOST}:{top_query.CH_PORT}'

HEADER = {
    'X-ClickHouse-Format': 'JSONColumns'
}


class ETL():
    
    def __init__(self, query, name_table, source, crushing_period, date_from, date_to) -> None:
        self.days = self.get_time_on_days(date_from, date_to + timedelta(days=1))
        self.mounts = self.get_time_on_months(date_from, date_to + timedelta(days=1))
        self.hours = self.get_time_on_hours(date_from, date_to + timedelta(days=1))
        self.source = source
        self.crushing_period = crushing_period
        self.main_query = query
        self.name_table = name_table


    def replase_querys_general(self, date_from, date_to):
        
        query = self.main_query
        source = self.source

        if source == 'MS':
            query = query.replace('{date_1}', date_from.strftime('%Y%m%d %H:%M:%S')).replace('{date_2}',
                                                                                        date_to.strftime('%Y%m%d %H:%M:%S'))
        else:
            query = query.replace('{date_1}', date_from.strftime('%Y-%m-%d %H:%M:%S')).replace('{date_2}',
                                                                                        date_to.strftime('%Y-%m-%d %H:%M:%S'))

        return query


    def create_partition(self, crushing_period):
        if crushing_period == 'mounts':
            for date_from, date_to in self.mounts:
                name_part = f"{self.name_table}_{date_from.strftime('%Y_%m_%d')}"
                
                query_add_part = f"""
                    CREATE TABLE {name_part} PARTITION OF {self.name_table}
                        FOR VALUES FROM ('{date_from}') TO ('{date_to}');
                """
                self.execute_part(query_add_part)
        
        if crushing_period == 'days':
            for date_from, date_to in self.days:
                name_part = f"{self.name_table}_{date_from.strftime('%Y_%m_%d')}"
                
                query_add_part = f"""
                    CREATE TABLE {name_part} PARTITION OF {self.name_table}
                        FOR VALUES FROM ('{date_from}') TO ('{date_to}');
                """
                self.execute_part(query_add_part)

        if crushing_period == 'hours':
            for date_from, date_to in self.hours:
                name_part = f"{self.name_table}_{date_from.strftime('%Y_%m_%d')}"
                
                query_add_part = f"""
                    CREATE TABLE {name_part} PARTITION OF {self.name_table}
                        FOR VALUES FROM ('{date_from}') TO ('{date_to}');
                """
                self.execute_part(query_add_part)

    def execute_part(self, query_add_part):
        try:
            engine.execute(query_add_part)
        except Exception as err:
            print(err)


    def get_time_on_hours(self, date_begin,date_end):
        hours = []
        while date_begin < date_end:
            hours.append((date_begin, date_begin.replace(minute=59,second=59)))
            date_begin = date_begin + timedelta(hours=1)

        return hours


    def get_time_on_days(self, date_begin, date_end):
        days = []
        while date_begin < date_end:
            days.append((date_begin, date_begin + timedelta(days=1)))
            date_begin = date_begin + timedelta(days=1)

        return days


    def get_time_on_months(self, date_begin,date_end):
        days = []
        date_begin = date_begin.replace(day=1)
        while date_begin < date_end:
            date_start = date_begin
            if date_begin.month == 12:
                date_begin = date_begin.replace(month=1, year=date_begin.year+1)
            else:
                date_begin = date_begin.replace(month=date_begin.month+1)

            days.append((date_start,date_begin))

        return days

    def _select_from_bd(self, query):

        df_new = pd.DataFrame()

        if self.source == 'MS':
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

    def load_data_part(self):
        if self.crushing_period == 'mounts':
            for date_from, date_to in self.mounts:
                sleep(5)
                query = self.replase_querys_general(date_from, date_to)
                df = self._select_from_bd(query)
                df.to_sql(self.name_table, engine, if_exists='append', index=False)

                print(date_from)
        
        if self.crushing_period == 'days':
            for date_from, date_to in self.days:
                sleep(5)
                query = self.replase_querys_general(date_from, date_to)
                df = self._select_from_bd(query)
                df.to_sql(self.name_table, engine, if_exists='append', index=False)

                print(date_from)

        if self.crushing_period == 'hours':
            for date_from, date_to in self.hours:
                sleep(5)
                query = self.replase_querys_general(date_from, date_to)
                df = self._select_from_bd(query)
                df.to_sql(self.name_table, engine, if_exists='append', index=False)

                print(date_from)


def get_header(days_):
    for date_from, date_to in days_:

        query = """
        SELECT 
            id_order, 
            date_order, 
            CONVERT(DATE, date_order) as date_part, 
            order_type, 
            number, 
            id_status, 
            source
        FROM Loyalty03..Orders_header WITH (NOLOCK)
        WHERE date_order between '{date_1}' and '{date_2}'
        """

        query = query.format(date_1=date_from.strftime('%Y%m%d'), date_2=date_to.strftime('%Y%m%d'))

        get_data_part(query=query, name_table='orders_header', date_from=date_from, date_to=date_to)


def get_order_line(days_):
    for date_from, date_to in days_:

        query = """
        SELECT 
            id_line, 
            CONVERT(DATE, date_order) as date_part, 
            Order_line.id_order as id_order, 
            id_tov, 
            quantity, 
            Order_line.base_sum as base_sum, 
            amount
        FROM Loyalty03..Orders_header WITH (NOLOCK)
        INNER JOIN Loyalty03..Order_line AS Order_line (NOLOCK) ON Order_line.id_order = Orders_header.id_order
        WHERE date_order between '{date_1}' and '{date_2}'
        """

        query = query.format(date_1=date_from.strftime('%Y%m%d'), date_2=date_to.strftime('%Y%m%d'))

        get_data_part(query=query, name_table='order_line', date_from=date_from, date_to=date_to)
        

def get_sebest_tov_tbl(days_):

    for date_from, date_to in days_:

        query = """
        SELECT 
            CAST(Sebest_tov_tbl._Period AS date) as date_month, 
            Sebest_tov_tbl.Tovar_Id as id_tov, 
            Sebest_tov_tbl.Sebestoimost as sebestoimost, 
            Sebest_tov_tbl.Sebestoimost_nds as sebestoimost_nds
        FROM NSI..Sebest_tov_tbl AS Sebest_tov_tbl (NOLOCK)
        WHERE CAST(Sebest_tov_tbl._Period AS date) = CAST('{date_1}' AS date)
        """

        query = query.format(date_1=date_from.strftime('%Y-%m-%d'))

        get_data_part(query=query, name_table='sebest_tov_tbl', date_from=date_from, date_to=date_to)


def get_tovs_search(date_begin, date_end):

    query = """
    SELECT 
        date_add,
        dates,
        weeks,
        id_tov,
        id_job,
        source,
        id_screen,
        id_element,
        id_search,
        number,
        segment,
        id_cart,
        device_type,
        id_version,
        action,
        search_bar,
        group_id,
        category_id,
        rn_green,
        rn_max,
        tovs_rn_real_sort,
        sort,
        filter_list,
        product_detail,
        greenmark,
        yellowmark,
        orangemark,
        redmark,
        greymark,
        bluemark,
        specialmark,
        chosenmark,
        button_name,
        value,
        list_id,
        list_title,
        qty,
        rn,
        name_tov,
        group_name,
        category_name,
        set_id,
        set_name,
        category_id_hierarchy,
        category_hierarchy,
        year,
        month,
        sebestoimost_nds,
        rn_sort,
        position,
        symbols,
        number_last
    FROM logs.tovs_search
    WHERE date_add between '{date_1}' and '{date_2}'
    """

    tovs_search = ETL(query, name_table='tovs_search', source='CH', crushing_period='hours', date_from=date_begin, date_to=date_end)
    tovs_search.create_partition(crushing_period='days')
    tovs_search.load_data_part()


def get_tovs_search(date_begin, date_end):

    query = """
    SELECT 
        date_add,
        dates,
        weeks,
        id_tov,
        id_job,
        source,
        id_screen,
        id_element,
        id_search,
        number,
        segment,
        id_cart,
        device_type,
        id_version,
        action,
        search_bar,
        group_id,
        category_id,
        rn_green,
        rn_max,
        tovs_rn_real_sort,
        sort,
        filter_list,
        product_detail,
        greenmark,
        yellowmark,
        orangemark,
        redmark,
        greymark,
        bluemark,
        specialmark,
        chosenmark,
        button_name,
        value,
        list_id,
        list_title,
        qty,
        rn,
        name_tov,
        group_name,
        category_name,
        set_id,
        set_name,
        category_id_hierarchy,
        category_hierarchy,
        year,
        month,
        sebestoimost_nds,
        rn_sort,
        position,
        symbols,
        number_last
    FROM logs.tovs_search
    WHERE date_add between '{date_1}' and '{date_2}'
    """

    tovs_search = ETL(query, name_table='tovs_search', source='CH', crushing_period='hours', date_from=date_begin, date_to=date_end)
    tovs_search.create_partition(crushing_period='days')
    tovs_search.load_data_part()


if __name__=='__main__':

    date_begin = datetime(2022,9,14,20)
    date_end = datetime(2022,9,14)

    # days_ = top_query.get_time_on_days(date_begin,date_end + timedelta(days=1))
    # month_ = get_months(date_begin,date_end + timedelta(days=1))
    # hours_ = get_hours(date_begin,date_end)
    # for date_from, date_to in days_:
    #     create_partition('tovs_search', date_from, date_to)

    # print(hours_)

    # get_header(days_)
    # get_order_line(days_)
    # get_sebest_tov_tbl(month_)
    get_tovs_search(date_begin, date_end)
