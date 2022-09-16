import pandas as pd

df = pd.read_parquet('tmp_cache/4/20220909_20220910_B_unique_numbers_bye_orders.parquet')

print(df['metric'].unique())


# from email.header import Header
# import pandas as pd
# import requests
# import json
# import copy
# from datetime import timedelta, datetime
# from dotenv import load_dotenv
# import os

# df = pd.read_parquet('all_answers/4/A_20220829_20220830_Количество клиентов, посещавших МП.parquet')
# print(df)

# dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
# if os.path.exists(dotenv_path):
#     load_dotenv(dotenv_path)

# MS_HOST_03 = os.getenv("MS_HOST_VV_03")
# MS_USER_03 = os.getenv("MS_USER_VV_03")
# MS_PASSWORD_03 = os.getenv("MS_PASSWORD_VV_03")
# MS_HOST_06 = os.getenv("MS_HOST_VV_06")
# CH_HOST = os.getenv("CH_HOST")
# CH_USER = os.getenv("CH_USER")
# CH_PASS = os.getenv("CH_PASS")
# CH_PORT = os.getenv("CH_PORT")

# def get_time_on_days(date_begin, date_end):
#     days = []
#     while date_begin < date_end:
#         days.append((date_begin, date_begin + timedelta(days=1)))
#         date_begin = date_begin + timedelta(days=1)

#     return days

# days_ = get_time_on_days(datetime.now() - timedelta(days=7), datetime.now())

# url = f'http://{CH_USER}:{CH_PASS}@{CH_HOST}:{CH_PORT}'

# q = "SELECT date_add, search_bar, id_search, number, id_element FROM logs.tovs_search WHERE date_add between '{date_1}' and '{date_2}';"
# df_new = pd.DataFrame()
# for date_from, date_to in days_:
#     query = q.format(
#             date_1=date_from.strftime('%Y-%m-%d'), 
#             date_2=date_to.strftime('%Y-%m-%d')
#         )
        
#     headers = {
#         'X-ClickHouse-Format': 'JSONColumns'
#     }

#     http = requests.post(
#         url=url, 
#         data=query, 
#         headers=headers
#         )

#     if http.status_code != 200:
#         continue

#     if df_new.empty:
#         df_new = pd.DataFrame(json.loads(http.text))
#     else:
#         df_c = pd.DataFrame(json.loads(http.text))
#         if not df_c.empty:
#             df_new = pd.concat([df_new,df_c])

# df_new.to_csv('123.csv', sep='\t', index=False, header=False)