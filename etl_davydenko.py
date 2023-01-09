# Блок импортов
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Данные для подключения к ClickHouse
connection = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'some_database',
                      'user':'student', 
                      'password':'some_password'
                     }


# Данные для подключения к схеме test
connection_test = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database':'test',
                      'user':'student',
                      'password':'some_password_test'
                     }

# задаем параметры для DAG

default_args = {
    'owner': 'a.davydenko',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 12, 12)
}

# Интервал запуска DAG каждый день в 11:00
schedule_interval = '0 11 * * *'

# DAG
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)  
def dag_a_davydenko():
    
     # Таск для выгрузки данных из feed_actions
    @task
    def extract_feed():
        query_1 = '''
        select DISTINCT toDate(time) AS event_date,
            user_id,
            countIf(action='like') as likes,
            countIf(action='view') as views,
            os,
            gender,
            age
        from {db}.feed_actions
        where toDate(time) == today() - 1
        group by event_date, user_id, user_id, os, gender, age
        '''
        
        feed_actions = ph.read_clickhouse(query_1, connection=connection)
        return feed_actions

    # Таск для выгрузки данных из message_actions
    @task
    def extract_message():
        query_2 = '''
        select event_date,
            user_id,
            messages_sent,
            messages_recieved,
            users_sent,
            users_recieved,
            os,
            gender,
            age
        from
            (select DISTINCT user_id,
                    toDate(time) AS event_date,
                    count(DISTINCT reciever_id) as users_sent,
                    count(reciever_id) as messages_sent,
                    os,
                    gender,
                    age
            from {db}.message_actions
            where toDate(time) == today() - 1
            group by event_date, user_id, os, gender, age) as q1
        join
            (select DISTINCT reciever_id, 
                    toDate(time) AS event_date,
                    count(DISTINCT user_id) as users_recieved,
                    count(user_id) as messages_recieved
            from {db}.message_actions 
            where toDate(time) == today() - 1 
            group by event_date, reciever_id) as q2
        on q1.user_id = q2. reciever_id
        '''

        message_actions = ph.read_clickhouse(query_2, connection=connection)
        return message_actions
    
    # объединение 2-х таблиц
    @task
    def transfrom_join(feed_actions, message_actions):
        df = feed_actions.merge(message_actions, how='outer', on=['event_date', 'user_id', 'gender', 'age', 'os']).fillna(0)
        return df

    # преобразование данных по срезу os
    @task
    def transform_os(df):
        df_os = df.groupby(by=['event_date', 'os'], as_index=False)[['event_date','os','views', 'likes', 'messages_sent', 'messages_recieved', 'users_sent', 'users_recieved']].sum()
        df_os['dimensions'] = 'OS'
        df_os.rename(columns={"os": "dimensions_value"}, inplace = True)
        df_os = df_os[['event_date', 'dimensions', 'dimensions_value','views', 'likes', 'messages_sent', 'messages_recieved', 'users_sent', 'users_recieved']]
                   
        return df_os

    # преобразование данных по срезу gender
    @task
    def transform_gender(df):
        df_gender = df.groupby(by=['event_date', 'gender'], as_index=False)[['views', 'likes', 'messages_sent', 'messages_recieved', 'users_sent', 'users_recieved']].sum()
        df_gender['dimensions'] = 'gender'
        df_gender.rename(columns={"gender": "dimensions_value"}, inplace = True)
        df_gender = df_gender[['event_date', 'dimensions', 'dimensions_value','views', 'likes', 'messages_sent', 'messages_recieved', 'users_sent', 'users_recieved']]
    
        return df_gender

    # преобразование данных по срезу age
    @task
    def transform_age(df):
        df_age = df.groupby(by=['event_date', 'age'], as_index=False)[['views', 'likes', 'messages_sent', 'messages_recieved', 'users_sent', 'users_recieved']].sum()
        df_age['dimensions'] = 'age'
        df_age.rename(columns={"age": "dimensions_value"}, inplace = True)
        df_age = df_age[['event_date', 'dimensions', 'dimensions_value','views', 'likes', 'messages_sent', 'messages_recieved', 'users_sent', 'users_recieved']]
            
        return df_age

    @task
    # объединяем 3 таблицы
    def transform_df_result(df_os, df_gender, df_age):
        df_result = pd.concat([df_os, df_gender, df_age], axis=0)
        df_result[['views', 'likes', 'messages_sent', 'messages_recieved', 'users_sent', 'users_recieved']] = df_result[['views', 'likes', 'messages_sent', 'messages_recieved', 'users_sent', 'users_recieved']].astype(int)
        
        return df_result
    
    @task
    # Таск загрузки результатов в базу test
    def load(df_result):
        # Создание таблицы в базе test, если её нет
        query = '''
        CREATE TABLE IF NOT EXISTS test.davydenko_airflow
            (
            event_date Date,
            dimensions varchar(50),
            dimensions_value varchar(50),
            views Int64,
            likes Int64,
            messages_sent Int64,
            messages_recieved Int64,
            users_sent Int64,
            users_recieved Int64
            ) ENGINE = Log()
        ORDER BY event_date
        '''
        ph.execute(query, connection=connection_test) 
        ph.to_clickhouse(df_result, 'davydenko_airflow', index=False, connection=connection_test)

    # выполням таски
    feed_actions = extract_feed()
    message_actions = extract_message()
    df = transfrom_join(feed_actions, message_actions)
    df_os = transform_os(df)
    df_gender = transform_gender(df)
    df_age = transform_age(df)
    df_result = transform_df_result(df_os, df_gender, df_age)
    load(df_result)
        
dag_a_davydenko = dag_a_davydenko()