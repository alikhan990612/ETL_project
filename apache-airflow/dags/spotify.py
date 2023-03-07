import oracledb as od
import airflow.providers.oracle
import pandas as pd 
from requests import get,post
import json
import datetime as dt
from airflow import DAG
from airflow.operators.python import PythonOperator

user = 'your db user'
password = 'your db password'
port = '1521'
service_name = 'localhost/XEPDB1'
conn_string = "oracle_server_addr:{port}/{service_name}".format(port=port, service_name=service_name)
connect = od.connect(user=user, password=password, dsn=service_name,port = port) 
my_data = pd.DataFrame()
load_data = []

dag = DAG(
    dag_id = 'Spotify_project',
    description= 'ETL - recently played tracks',
    start_date = dt.datetime(2023,3,4),
    schedule = '@daily'
)

# Создаю функцию который авторизуется через токен. Потом берет названия песни который слушал, дату релиза, дату прослушивания и заливаю все данные в список
# После создаю dataframe из списков.
def extract_data():
    music_name = []
    release_date = []
    played_at = []
    artist_name = []
    url = 'https://api.spotify.com/v1/me/player/recently-played?limit=10'       # Все прослушанные песни с лимитом 10 песен за раз
    headers = {'Authorization': 'Bearer BQCFwhBcg5hlFw3HIIKYUKFfn_BViIWacNTtd9AIYuDsE1i2KpWlNSW1dvk4a0H_01K2ibu9FriHUF0cy1fCx_M_30RobCNqAleeguJ7jGW_XssnXZKRnmr7E-TJ-hL_6958GF9n4MtWOBGGlcl4r1c6UM5NWGFX004LCkvf9Ke9HExOz2a8POp2i5Z1aCancXC2GlUGQA'}
    r = get(url,headers = headers)  # Используя get авторизуюсь на сервисе spotify
    r_j = r.json()                  # Преобразую в тип json 
    for i in r_j['items']:
        #print(i['track']['album']['release_date'],i['track']['name'],i['played_at'],i['track']['artists'][0]['name'])
        music_name.append(i['track']['name'])
        release_date.append(i['track']['album']['release_date'])
        played_at.append(i['played_at'])
    my_data = my_data.assign(Music_name = music_name, Played_at = played_at, Release_date = release_date)

# Создаю таск с вызовом функций extract_data
task_1 = PythonOperator(
    task_id = 'Extracting_data',
    python_callable = extract_data,
    dag = dag
)

# Здесь проверяю пусто ли dataframe или нет. Имееть ли np.nan значения. Имееть ли дублирующие данные
# После через map из столбца played_at убираю лишние буквы
# В конце беру значения из dataframe и заполняю все значения в новый список load_data преобразуя внутренние списки в кортежи 
def transform_data():
    if my_data.empty:
        print('DataFrame is null')
    if my_data.Played_at.value_counts().values.max() > 1:
        print('DataFrame has not unique values')
    else:
        pass
    if my_data.isnull().values.any():
        print('DataFrame has null values')
    my_data.Played_at = my_data.Played_at.map(lambda x : x.replace('T',' ').replace('Z',''))
    #my_data.Played_at = pd.to_datetime(my_data.Played_at)
    #print(my_data[['Played_at','Music_name']])
    #my_data.Played_at = my_data.Played_at.dt.tz_localize('UTC').dt.tz_convert('Asia/Aqtobe')
    #my_data.Played_at = pd.to_datetime(my_data.Played_at)
    sql_select = connect.cursor()
    for i in sql_select.execute('select max(played_at) from wt_spotify_pt'):
        max_date = i
    for i in range(len(my_data.values)):
        load_data.append(tuple(my_data.values[i]))

# Создаю таск с вызовом функций transform_data
task_2 = PythonOperator(
    task_id = 'Transforming_data',
    python_callable = transform_data,
    dag = dag
)

# Создаю курсор из подключенной базы. После заполняю таблицу указывая порядок столбцов. Обратите внимание, здесь не указываю столбец data_id
def loading_Data():
    sql_insert = connect.cursor()
    sql_insert.executemany('insert into wt_spotify_pt (music_name,played_at,release_date) values (:2,:3,:4)', load_data)
    sql_insert.commit()

task_3 = PythonOperator(
    task_id = 'Loading_data_to_oracledb',
    python_callable=loading_Data,
    dag = dag
)

# Запускаю все таски определяя порядок
task_1 >> task_2 >> task_3

