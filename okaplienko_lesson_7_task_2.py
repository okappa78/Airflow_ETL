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

from io import StringIO
import requests

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

# Определяем имя таблицы
mytable_name = 'okaplienko_sumtable_bot'

# Пустой словарь для создания таблиц
empty_dict = {'event_date': [],
            'user_id': [],
            'os': [],
            'gender': [],
            'source': [],
            'views': [],
            'likes': [],
            'messages_received': [],
            'messages_sent': [],
            'users_received': [],
            'users_sent': []}


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
    query_check = f'''select distinct event_date
                 from test.{mytable_name}
                 
                 format TSVWithNames'''
    
    df_check = ch_get_df(query=query_check,
                         host='https://clickhouse.lab.karpov.courses',
                         user='student-rw',
                         password='656e2b0c9c')
    
    yesterday_day = (date.today() - timedelta(1)).isoformat()
    check_result = yesterday_day in list(df_check.event_date)
    return check_result

# Определяем функцию построения графиков
def metrics_drawing(df, title, *metrics):
    sns.set_style("whitegrid")
    plt.figure(figsize=(12, 6))
    for metric in metrics:
        sns.lineplot(data=df, x="event_date", y=metric, label=metric)
    plt.title(title, fontsize=24)
    plt.xlabel('')
    plt.ylabel('')
    plot_object = io.BytesIO()
    plt.savefig(plot_object)
    plot_object.seek(0)
    plot_object.name = 'test_plot.png'
    plt.close()
    return plot_object

# Определяем функцию отправки фалйа
def sendfile(df, chat_id=chat_id):
    file_object = io.StringIO()
    df.to_csv(file_object)
    file_object.name = 'summary_table.csv'
    file_object.seek(0)
    bot.sendDocument(chat_id=chat_id, document=file_object)

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_kaplienko_summary_bot():
    
    @task
    def extract():
        check_date_result = check_date()
        if check_date_result:
            df_empty = pd.DataFrame(data=empty_dict)
            return df_empty
        
        
        connection = {
              'host': 'https://clickhouse.lab.karpov.courses',
              'database':'simulator_20230220',
              'user':'student', 
              'password':'dpo_python_2020'
              }
        query = '''
        select event_date,
               user_id,
               os,
               gender,
               source,
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

        full outer join

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
                        source
        from simulator_20230220.message_actions

        union distinct

        select distinct user_id,
                        os,
                        gender,
                        source
        from simulator_20230220.feed_actions
        ) t22

        using user_id
        '''
        df = ph.read_clickhouse(query, connection=connection)
        return df
    
    # Определяем функцию расчета и добавления DAU
    @task
    def dau_calculation(df):
        df = df.assign(dau_all = True)
        df = df.assign(dau_news = (df.views != 0) * (df.messages_sent == 0))
        df = df.assign(dau_news_messages = (df.views != 0) * (df.messages_sent != 0))
        df = df.assign(dau_messages = (df.views == 0) * (df.messages_sent != 0))
        return df
    
    # Определяем функцию расчета показателей в разрезе os
    @task
    def transfrom_os(df):
        df_os = df[['event_date',
                    'os',
                    'views',
                    'likes',
                    'messages_received',
                    'messages_sent',
                    'users_received',
                    'users_sent',
                    'dau_all',
                    'dau_news',
                    'dau_news_messages',
                    'dau_messages']] \
            .groupby(['event_date', 'os'])\
            .sum()\
            .reset_index()
        df_os.insert(1, 'dimension', 'os')
        df_os = df_os.rename(columns={'os': 'dimension_value'})
        return df_os
    
    # Определяем функцию расчета показателей в разрезе gender
    @task
    def transfrom_gender(df):
        df_gender= df[['event_date',
                       'gender',
                       'views',
                       'likes',
                       'messages_received',
                       'messages_sent',
                       'users_received',
                       'users_sent',
                       'dau_all',
                       'dau_news',
                       'dau_news_messages',
                       'dau_messages']] \
            .groupby(['event_date', 'gender'])\
            .sum()\
            .reset_index()
        df_gender.insert(1, 'dimension', 'gender')
        df_gender = df_gender.rename(columns={'gender': 'dimension_value'})
        return df_gender
    
    # Определяем функцию расчета показателей в разрезе source
    @task
    def transfrom_source(df):
        df_source = df[['event_date',
                        'source',
                        'views',
                        'likes',
                        'messages_received',
                        'messages_sent',
                        'users_received',
                        'users_sent',
                        'dau_all',
                        'dau_news',
                        'dau_news_messages',
                        'dau_messages']] \
            .groupby(['event_date', 'source'])\
            .sum()\
            .reset_index()
        df_source.insert(1, 'dimension', 'source')
        df_source = df_source.rename(columns={'source': 'dimension_value'})
        return df_source
    
    # Определяем функцию обновления таблицы
    @task
    def upload(df_os, df_gender, df_source):
        connection = {
                  'host': 'https://clickhouse.lab.karpov.courses',
                  'database':'test',
                  'user':'student-rw', 
                  'password':'656e2b0c9c'
                  }

        df_result = pd.concat([df_os, df_gender, df_source], ignore_index=True)
        df_result['active_per_user'] = round((df_result.views + df_result.likes + df_result.messages_sent) / df_result.dau_all, 2)
        df_result['event_date'] = pd.to_datetime(df_result['event_date'])
        ph.to_clickhouse(df=df_result, table=mytable_name, index=False, connection=connection)
        
        # Выгружаем таблицу с данными за прошедшие 7 дней
        query = f'''select * 
                    from test.{mytable_name} 
                    where event_date >= today() - 7'''
        df = ph.read_clickhouse(query, connection=connection)
        df = df.sort_values(by=['event_date', 'dimension'], ascending=False)
        return df
    
    # Определяем фунцию расчета показателей с разбивкой по дням
    @task
    def transform_day(df):
        df_result = df[df.dimension == 'source'][['event_date',
                                                  'views',
                                                  'likes',
                                                  'messages_received',
                                                  'messages_sent',
                                                  'users_received',
                                                  'users_sent',
                                                  'dau_all',
                                                  'dau_news',
                                                  'dau_news_messages',
                                                  'dau_messages']] \
                                        .groupby(['event_date']) \
                                        .sum() \
                                        .sort_values('event_date', ascending=False) \
                                        .reset_index()
        df_result['active_per_user'] = round((df_result.views + df_result.likes + df_result.messages_sent) / df_result.dau_all, 2)
        return df_result
    
    @task
    def sendmsg(df, chat_id=chat_id):
        # Определяем вчерашнюю дату
        yesterday_day = df.loc[0].event_date.strftime('%d-%m-%Y')
        
        # Определяем значения для передачи в чат
        dau_all = f'{df.loc[0].dau_all:_}'.replace("_", "'")
        dau_news = f'{df.loc[0].dau_news:_}'.replace("_", "'")
        dau_news_messages = f'{df.loc[0].dau_news_messages:_}'.replace("_", "'")
        dau_messages = f'{df.loc[0].dau_messages:_}'.replace("_", "'")
        views = f'{df.loc[0].views:_}'.replace("_", "'")
        likes = f'{df.loc[0].likes:_}'.replace("_", "'")
        messages_sent = f'{df.loc[0].messages_sent:_}'.replace("_", "'")
        active_per_user = df.loc[0].active_per_user

        msg = f'''
Доброе утро!
Значение ключевых метрик
по всему приложению (Новости&Сообщения)
за предыдущий день {yesterday_day}:
* DAU: {dau_all}
        из них использовавших
        - только Новости: {dau_news}
        - Новости и Сообщения: {dau_news_messages}
        - только Сообщения: {dau_messages}
* Просмотры: {views}
* Лайки: {likes}
* Сообщения:{messages_sent}
* Действий на 1 пользователя: {active_per_user}
        '''
        bot.sendMessage(chat_id=chat_id, text=msg)
        return df
    
    @task
    def sendplot_file(df_plot, df_file, chat_id=chat_id):
        yesterday_day_1 = df_plot.iloc[0].event_date.strftime('%d-%m-%Y')
        yesterday_day_7 = df_plot.iloc[-1].event_date.strftime('%d-%m-%Y')
        bot.sendMessage(chat_id=chat_id, text=f'''Графики ключевых метрик
по всему приложению (Новости&Сообщения)
за предыдущие 7 дней:
с {yesterday_day_7} по {yesterday_day_1}''')
                
        plot_object = metrics_drawing(df_plot, 'DAU', 'dau_news', 'dau_messages', 'dau_news_messages')
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        plot_object = metrics_drawing(df_plot, 'Активность', 'views', 'likes', 'messages_sent')
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        plot_object = metrics_drawing(df_plot, 'Активность на 1 пользователя', 'active_per_user')
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        
        sendfile(df_file)
        

        
    df = extract()
    df_cube = dau_calculation(df)
    df_cube_os = transfrom_os(df_cube)
    df_cube_gender = transfrom_gender(df_cube)
    df_cube_source = transfrom_source(df_cube)
    df_cube_7 = upload(df_cube_os, df_cube_gender, df_cube_source)
    df_cube_result = transform_day(df_cube_7)
    df_msg = sendmsg(df_cube_result)
    sendplot_file(df_msg, df_cube_7)
    
dag_kaplienko_summary_bot = dag_kaplienko_summary_bot()