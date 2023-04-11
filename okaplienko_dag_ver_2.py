# Импортируем библиотеки
from datetime import date, datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Определяем функцию для получения данных из clickhouse
def ch_get_df(query='Select 1',
              host='https://clickhouse.lab.karpov.courses',
              user='student',
              password='dpo_python_2020'):
    
    r = requests.post(host, data=query.encode("utf-8"), auth=(user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep='\t')
    return result

# Функция проверки даты
def check_date():
    connection = {
                  'host': 'https://clickhouse.lab.karpov.courses',
                  'database':'test',
                  'user':'student-rw', 
                  'password':'656e2b0c9c'
                  }
    
    query_create = '''CREATE TABLE IF NOT EXISTS test.okaplienko_table_report
                    (event_date Date,
                     dimension String,
                     dimension_value String,
                     views Int64,
                     likes Int64,
                     messages_received Int64,
                     messages_sent Int64,
                     users_received Int64,
                     users_sent Int64)
                    ENGINE MergeTree()
                    ORDER BY event_date'''
    ph.execute(query=query_create, connection=connection)
    
    query_check = '''select distinct event_date
                 from test.okaplienko_table_report
                 format TSVWithNames'''
    df_check = ch_get_df(query=query_check,
                         host='https://clickhouse.lab.karpov.courses',
                         user='student-rw',
                         password='656e2b0c9c')
    yesterday_day = (date.today() - timedelta(1)).isoformat()
    check_result = yesterday_day in list(df_check.event_date)
    return check_result

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'o.kaplienko',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 10),
}

# Интервал запуска DAG
schedule_interval = '0 7 * * *'


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_report_kaplienko_180323():
    
    @task()
    def extract():
        check_date_result = check_date()
        if check_date_result:
            empty_dict = {'event_date': [],
            'user_id': [],
            'os': [],
            'gender': [],
            'age': [],
            'views': [],
            'likes': [],
            'messages_received': [],
            'messages_sent': [],
            'users_received': [],
            'users_sent': []}
            df_cube_empty = pd.DataFrame(data=empty_dict)
            return df_cube_empty
        
        query = """select event_date,
                           user_id,
                           os,
                           gender,
                           age,
                           views,
                           likes,
                           messages_received,
                           messages_sent,
                           users_received,
                           users_sent
                    from
                    (
                    select today() - 1 event_date,
                           user_id,
                           views,
                           likes,
                           messages_received,
                           messages_sent,
                           users_received,
                           users_sent
                    from
                    (
                    select user_id,
                           sum(action='view') views,
                           sum(action='like') likes
                    from simulator_20230220.feed_actions
                    where toDate(time) = today() - 1
                    group by user_id) t11

                    full outer join

                    (select user_id,
                            messages_received,
                            messages_sent,
                            users_received,
                            users_sent
                    from
                    (select user_id,
                            count(reciever_id) messages_sent,
                            count(distinct reciever_id) users_sent
                    from simulator_20230220.message_actions
                    where toDate(time) = today() - 1
                    group by user_id) t1

                    join

                    (select reciever_id,
                            count(user_id) messages_received,
                            count(distinct user_id) users_received
                    from simulator_20230220.message_actions
                    where toDate(time) = today() - 1
                    group by reciever_id) t2

                    on user_id = reciever_id) t12

                    using user_id
                    ) t21

                    join

                    (
                    select distinct user_id,
                                    os,
                                    gender,
                                    age
                    from simulator_20230220.message_actions

                    union distinct

                    select distinct user_id,
                                    os,
                                    gender,
                                    age
                    from simulator_20230220.feed_actions
                    ) t22

                    using user_id

                    format TSVWithNames"""
        df_cube = ch_get_df(query=query)
        return df_cube
    
    
    @task
    def transfrom_os(df_cube):
        df_cube_os = df_cube[['event_date',
                              'os',
                              'views',
                              'likes',
                              'messages_received',
                              'messages_sent',
                              'users_received',
                              'users_sent']] \
            .groupby(['event_date', 'os'])\
            .sum()\
            .reset_index()
        df_cube_os.insert(1, 'dimension', 'os')
        df_cube_os = df_cube_os.rename(columns={'os': 'dimension_value'})
        return df_cube_os
    
    @task
    def transfrom_gender(df_cube):
        df_cube_gender = df_cube[['event_date',
                              'gender',
                              'views',
                              'likes',
                              'messages_received',
                              'messages_sent',
                              'users_received',
                              'users_sent']] \
            .groupby(['event_date', 'gender'])\
            .sum()\
            .reset_index()
        df_cube_gender.insert(1, 'dimension', 'gender')
        df_cube_gender = df_cube_gender.rename(columns={'gender': 'dimension_value'})
        return df_cube_gender
    
    @task
    def transfrom_age(df_cube):
        df_cube_age = df_cube[['event_date',
                              'age',
                              'views',
                              'likes',
                              'messages_received',
                              'messages_sent',
                              'users_received',
                              'users_sent']] \
            .groupby(['event_date', 'age'])\
            .sum()\
            .reset_index()
        df_cube_age.insert(1, 'dimension', 'age')
        df_cube_age = df_cube_age.rename(columns={'age': 'dimension_value'})
        return df_cube_age
    
    @task
    def load(df_cube_os, df_cube_gender, df_cube_age):
        connection = {
                  'host': 'https://clickhouse.lab.karpov.courses',
                  'database':'test',
                  'user':'student-rw', 
                  'password':'656e2b0c9c'
                  }

        df_cube_result = pd.concat([df_cube_os, df_cube_gender, df_cube_age], ignore_index=True)
        df_cube_result['event_date'] = pd.to_datetime(df_cube_result['event_date'])
        ph.to_clickhouse(df=df_cube_result, table='okaplienko_table_report', index=False, connection=connection)
        
        
    df_cube = extract()
    df_cube_os = transfrom_os(df_cube)
    df_cube_gender = transfrom_gender(df_cube)
    df_cube_age = transfrom_age(df_cube)
    load(df_cube_os, df_cube_gender, df_cube_age)
    
dag_report_kaplienko_180323 = dag_report_kaplienko_180323()
