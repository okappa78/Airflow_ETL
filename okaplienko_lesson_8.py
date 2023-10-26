# Импортируем библиотеки
from datetime import date, datetime, timedelta
import pandas as pd
import requests
import pandahouse as ph
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io

from airflow.decorators import dag, task

# Получаем доступ к боту и определяем чат, в который будем отправлять сообщения
my_token = '6104576053:AAE2qC3fRmaf5Zo1YvmIILyWNOp8scg4Q0g' 
bot = telegram.Bot(token=my_token)
chat_id =  -958942131 #153370571 -802518328 -958942131

# Определяем функцию для сообщения в мессенджер
def daylst(day_start, day_finish):
    day_delta = day_finish - day_start
    if day_delta == 0:
        lst_days = 'последний час'
    elif day_delta == 1:
        lst_days = 'аналогичный период текущего и вчерашнего дней'
    else:
        lst_days = f'аналогичный период текущего и прошедших {day_delta} дней'
    return lst_days

# Определяем функцию расчета доверительного интервала
def get_interval(data):
    q25 = data.quantile(.25)
    q75 = data.quantile(.75)
    IQR = q75 - q25
    xmin = q25 - 1.5 * IQR
    xmax = q75 + 1.5 * IQR
    return xmin, xmax

# Определяем функцию проверки отклонения для показаний последнего часа за несколько дней
# day_start - день, которым начинается интервал, с которым будет проводиться сравнение
# day_finish день, которым заканчивается интервал, В ИНТЕРВАЛ НЕ ВХОДИТ
def check_anomaly(df, metric, day_start=0, day_finish=7):
    is_alert, diff = 0, 0
    current_value = df[metric].iloc[0]
    
    # загружаем данные последних 60 минут
    df_result = df[metric].iloc[1: 5]
    # загружаем данные из предыдущих дней: 60 минут до и 60 после
    for tdif in range(day_start + 1, day_finish):
        index_start = tdif * 96 - 3
        index_finish = tdif * 96 + 5
        df_last_hour = df[metric].iloc[index_start: index_finish]
        df_result = pd.concat([df_result, df_last_hour])
    
    xmin, xmax = get_interval(df_result)
    
    if current_value < xmin or current_value > xmax:
        is_alert = 1
        diff = round(current_value / df_result.median() * 100 - 100, 2)
        return is_alert, current_value, diff
    
    return is_alert, current_value, diff

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'o.kaplienko',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2023, 3, 26),
}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_kaplienko_alerts_bot():
    
    @task
    def extract():
        connection = {
              'host': 'https://clickhouse.lab.karpov.courses',
              'database':'simulator_20230220',
              'user':'student', 
              'password':'dpo_python_2020'
              }
        # Количество дней, из которых будем получать данные для расчета доверительного интервала
        ndays = 7
        # Расчет временной разницы в минутах, при которой последние четыре элемента в полученном датафрейме,
        # которые всегда будут соответствовать последнему часу с отнесением на заданное количество дней
        minutes_past = 90 + ndays * 24 * 60
        
        query = f'''select ts,
                           date,
                           hm,
                           users_lenta,
                           views,
                           likes,
                           ctr,
                           users_messages,
                           messages_sent
                    FROM
                    (SELECT toStartOfFifteenMinutes(time) ts,
                                            toDate(ts) date,
                                            formatDateTime(ts, '%R') hm,
                                            uniqExact(user_id) users_lenta,
                                            sum(action='view') views,
                                            sum(action='like') likes,
                                            likes / views ctr
                                    FROM simulator_20230220.feed_actions
                                    WHERE ts >=  DateAdd(mi, -{minutes_past}, now()) 
                                          and ts < toStartOfFifteenMinutes(now())
                                    GROUP BY ts, date, hm
                                    ORDER BY ts desc) t1

                    join 

                    (SELECT toStartOfFifteenMinutes(time) ts,
                                            toDate(ts) date,
                                            formatDateTime(ts, '%R') hm,
                                            uniqExact(user_id) users_messages,
                                            count(user_id) messages_sent
                                    FROM simulator_20230220.message_actions
                                    WHERE ts >=  DateAdd(mi, -{minutes_past}, now()) 
                                          and ts < toStartOfFifteenMinutes(now())
                                    GROUP BY ts, date, hm
                                    ORDER BY ts desc) t2
                    using ts'''
        df = ph.read_clickhouse(query, connection=connection)
        return df
    
    @task
    def check_anomaly_usernews(df):
        metric = 'users_lenta'
        day_start, day_finish = 0, 2
        link = 'http://superset.lab.karpov.courses/r/3307'
        
        is_alert, current_value, diff = check_anomaly(df, metric, day_start=day_start, day_finish=day_finish)

        if is_alert:
            lst_days = daylst(day_start, day_finish)
            msg = f'''
!!!Метрика {metric}!!!
Текущее значение {current_value}
Отклонение относительно медианы за {lst_days} составлет {diff}%
{link}'''
            bot.sendMessage(chat_id=chat_id, text=msg)

    @task
    def check_anomaly_views(df):
        metric = 'views'
        day_start, day_finish = 0, 2
        link = 'http://superset.lab.karpov.courses/r/3308'
        
        is_alert, current_value, diff = check_anomaly(df, metric, day_start=day_start, day_finish=day_finish)

        if is_alert:
            lst_days = daylst(day_start, day_finish)
            msg = f'''
!!!Метрика {metric}!!!
Текущее значение {current_value}
Отклонение относительно медианы за {lst_days} составлет {diff}%
{link}'''
            bot.sendMessage(chat_id=chat_id, text=msg)
    
    
    @task
    def check_anomaly_likes(df):
        metric = 'likes'
        day_start, day_finish = 0, 2
        link = 'http://superset.lab.karpov.courses/r/3308'
        
        is_alert, current_value, diff = check_anomaly(df, metric, day_start=day_start, day_finish=day_finish)

        if is_alert:
            lst_days = daylst(day_start, day_finish)
            msg = f'''
!!!Метрика {metric}!!!
Текущее значение {current_value}
Отклонение относительно медианы за {lst_days} составлет {diff}%
{link}'''
            bot.sendMessage(chat_id=chat_id, text=msg)
            
    @task
    def check_anomaly_ctr(df):
        metric = 'ctr'
        day_start, day_finish = 0, 7
        link = 'http://superset.lab.karpov.courses/r/3309'
        
        is_alert, current_value, diff = check_anomaly(df, metric, day_start=day_start, day_finish=day_finish)

        if is_alert:
            lst_days = daylst(day_start, day_finish)
            msg = f'''
!!!Метрика {metric}!!!
Текущее значение {current_value}
Отклонение относительно медианы за {lst_days} составлет {diff}%
{link}'''
            bot.sendMessage(chat_id=chat_id, text=msg)
            
    @task
    def check_anomaly_usersmessages(df):
        metric = 'users_messages'
        day_start, day_finish = 0, 7
        link = 'http://superset.lab.karpov.courses/r/3310'
        
        is_alert, current_value, diff = check_anomaly(df, metric, day_start=day_start, day_finish=day_finish)

        if is_alert:
            lst_days = daylst(day_start, day_finish)
            msg = f'''
!!!Метрика {metric}!!!
Текущее значение {current_value}
Отклонение относительно медианы за {lst_days} составлет {diff}%
{link}'''
            bot.sendMessage(chat_id=chat_id, text=msg)
            
    @task
    def check_anomaly_messagessent(df):
        metric = 'messages_sent'
        day_start, day_finish = 0, 7
        link = 'http://superset.lab.karpov.courses/r/3311'
        
        is_alert, current_value, diff = check_anomaly(df, metric, day_start=day_start, day_finish=day_finish)

        if is_alert:
            lst_days = daylst(day_start, day_finish)
            msg = f'''
!!!Метрика {metric}!!!
Текущее значение {current_value}
Отклонение относительно медианы за {lst_days} составлет {diff}%
{link}'''
            bot.sendMessage(chat_id=chat_id, text=msg)
            
    df_cube = extract()
    
    check_anomaly_usernews(df_cube)
    check_anomaly_views(df_cube)
    check_anomaly_likes(df_cube)
    check_anomaly_ctr(df_cube)
    
    check_anomaly_usersmessages(df_cube)
    check_anomaly_messagessent(df_cube)
    
dag_kaplienko_alerts_bot = dag_kaplienko_alerts_bot()
