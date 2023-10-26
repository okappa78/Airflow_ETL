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
chat_id = -802518328

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'o.kaplienko',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 3, 20),
}

# Интервал запуска DAG
schedule_interval = '59 10 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_kaplienko_bot_news():
    
    @task
    def extract():
        connection = {
              'host': 'https://clickhouse.lab.karpov.courses',
              'database':'simulator_20230220',
              'user':'student', 
              'password':'dpo_python_2020'
              }
        query = '''
        select toDate(time) event_date,
               count(distinct user_id) dau,
               sum(action='view') views,
               sum(action='like') likes,
               sum(action='like') / sum(action='view') ctr
        from simulator_20230220.feed_actions
        where toDate(time) <= today() - 1 
              and toDate(time) >= today() - 7
        group by event_date
        order by event_date desc
        '''
        df = ph.read_clickhouse(query, connection=connection)
        return df
    
    # Определяем функциб для передачи сообщений
    @task
    def sendmsg(df, chat_id=chat_id):
        # Определяем вчерашнюю дату
        yesterday_day = df.loc[0].event_date.strftime('%d-%m-%Y')
        
        # Определяем значения для передачи в чат
        dau = f'{df.loc[0].dau:_}'.replace("_", "'")
        views = f'{df.loc[0].views:_}'.replace("_", "'")
        likes = f'{df.loc[0].likes:_}'.replace("_", "'")
        ctr = round(df.loc[0].ctr, 4)

        msg = f'''
Доброе утро!
Значение ключевых метрик за предыдущий день {yesterday_day}:
- DAU: {dau}
- Просмотры: {views}
- Лайки: {likes}
- CTR: {ctr}
        '''
        bot.sendMessage(chat_id=chat_id, text=msg)
        return df
     
    # Определяем функцию, для передачи графика
    @task
    def sendplot(df, chat_id=chat_id):
        yesterday_day_1 = df.loc[0].event_date.strftime('%d-%m-%Y')
        yesterday_day_7 = df.loc[6].event_date.strftime('%d-%m-%Y')
        bot.sendMessage(chat_id=chat_id, text=f'''Графики ключевых метрик за предыдущие 7 дней:
с {yesterday_day_7} по {yesterday_day_1}''')
        
        metrics = list(df.columns.values)[1:]
        titles = ['DAU', 'Просмотры', 'Лайки', 'CTR']
        dict_titles = dict(zip(metrics, titles))
        
        for key in metrics:
            sns.set_style("whitegrid")
            plt.figure(figsize=(12, 6))
            sns.lineplot(data=df, x='event_date', y=key)
            plt.title(f'{dict_titles[key]}', fontsize=24)
            plt.xlabel('')
            plt.ylabel('')
            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = 'test_plot.png'
            plt.close()
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    
    df_01 = extract()
    df_02 = sendmsg(df_01)
    sendplot(df_02)

    
dag_kaplienko_bot_news = dag_kaplienko_bot_news()