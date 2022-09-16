from calendar import month
import body
from datetime import datetime, timedelta
from bcp_load import DataManipulation
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from mssql import MsSQL
from copy import deepcopy


sourсe_06 = MsSQL(
            params=deepcopy(body.ms_c_06)
        )


con_str = f'postgresql://{body.PG_USER}:{body.PG_PASS}@{body.PG_HOST}/{body.PG_SCHEMA}'
engine = create_engine(con_str, poolclass=NullPool, isolation_level='AUTOCOMMIT')

def get_data_part(query, name_table, date_from, date_to):
    name_part = f"{name_table}_{date_from.strftime('%Y_%m_%d')}"
    
    query_add_part = f"""
        CREATE TABLE {name_part} PARTITION OF {name_table}
            FOR VALUES FROM ('{date_from}') TO ('{date_to}');
    """

    try:
        engine.execute(query_add_part)
    except Exception as err:
        print(err)
        return

    # bcp_ = DataManipulation()

    # bcp_.extract_data_bcp(query=query, ms=body.ms_c_06)

    df = sourсe_06.select_to_df(query)
    df.to_sql(name_table, engine, if_exists='append', index=False)

    print(date_from)


def get_months(date_begin,date_end):
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

def get_header(days_):
    for date_from, date_to in days_:

        query = "SELECT id_order, date_order, CONVERT(DATE, date_order) as date_part, order_type, number, id_status, source " \
            "FROM Loyalty03..Orders_header WITH (NOLOCK) " \
            "WHERE date_order between '{date_1}' and '{date_2}'"

        query = query.format(date_1=date_from.strftime('%Y%m%d'), date_2=date_to.strftime('%Y%m%d'))

        get_data_part(query=query, name_table='orders_header', date_from=date_from, date_to=date_to)


def get_order_line(days_):
    for date_from, date_to in days_:

        query = "SELECT id_line, CONVERT(DATE, date_order) as date_part, Order_line.id_order as id_order, id_tov, quantity, Order_line.base_sum as base_sum, amount " \
            "FROM Loyalty03..Orders_header WITH (NOLOCK) " \
            "INNER JOIN Loyalty03..Order_line AS Order_line (NOLOCK) ON Order_line.id_order = Orders_header.id_order " \
            "WHERE date_order between '{date_1}' and '{date_2}'"

        query = query.format(date_1=date_from.strftime('%Y%m%d'), date_2=date_to.strftime('%Y%m%d'))

        get_data_part(query=query, name_table='order_line', date_from=date_from, date_to=date_to)
        

def get_sebest_tov_tbl(days_):

    for date_from, date_to in days_:

        query = "SELECT CAST(Sebest_tov_tbl._Period AS date) as date_month, Sebest_tov_tbl.Tovar_Id as id_tov, Sebest_tov_tbl.Sebestoimost as sebestoimost, Sebest_tov_tbl.Sebestoimost_nds as sebestoimost_nds " \
            "FROM NSI..Sebest_tov_tbl AS Sebest_tov_tbl (NOLOCK) "\
            "WHERE CAST(Sebest_tov_tbl._Period AS date) = CAST('{date_1}' AS date)"

        query = query.format(date_1=date_from.strftime('%Y-%m-%d'))

        get_data_part(query=query, name_table='sebest_tov_tbl', date_from=date_from, date_to=date_to)



if __name__=='__main__':

    date_begin = datetime(2022,8,29)
    date_end = datetime(2022,8,30)

    days_ = body.get_time_on_days(date_begin,date_end + timedelta(days=1))
    month_ = get_months(date_begin,date_end + timedelta(days=1))

    get_header(days_)
    get_order_line(days_)
    get_sebest_tov_tbl(month_)


        
