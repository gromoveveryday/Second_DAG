from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import get_current_context
import requests
import pandas as pd
import pandahouse
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.pyplot import figure
import telegram
import io

# Импортируем все необходимые библиотеки

connection1 = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'database': 'simulator_20221220',
    'user': 'student',
    'password': 'dpo_python_2020'
}

# Задаем словарь для подключения к БД Clickhouse

default_args = {
    'owner': 'i-gromov',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 16),
}

# Задаем словарь с основными параметрами для настройки DAG'а

schedule_interval = '0 11 * * *'

# Каждые 11 часов утра по московскому времени DAG будет срабатывать

bot_token = '5805207394:AAEl7-_Oqdh90P7jniulur3E9Y7DDSjz9Og'
bot = telegram.Bot(token = bot_token)
chat_id = -850804180

# Для создания DAG'а был создан бот, имеющий следующий токен, также задан id чата, в которой будет приходить сообщение от бота

@dag(default_args = default_args, schedule_interval = schedule_interval, catchup = False)
def i_gromov_dag_task_7_1():
    @task()
    def extract_1():
        query1 = """
        SELECT toDate(time) as date,
        COUNT(DISTINCT user_id) AS DAU,
        countIf(user_id, action='like') AS likes,
        countIf(user_id, action='view') AS views,
        ROUND(countIf(user_id, action='like') / countIf(user_id, action='view'), 6) AS CTR
        FROM simulator_20221220.feed_actions
        WHERE toDate(time) BETWEEN yesterday() - 6 AND yesterday()
        GROUP BY date"""
        df1 = pandahouse.read_clickhouse(query1, connection = connection1)
        return df1

# Сделаем запрос к БД и создадим датафрейм, в нем буду содержаться основыне продуктовые метрики, сгруппированные по дате за последние 7 дней

    @task()
    def send_message_2_1(df1, chat_id):
        yestarday_date = datetime.now() - timedelta(days=1)
        message = f"Ключевые показатели за вчерашний день ({yestarday_date}) следующие:\nDAU: {df1.iloc[-1].DAU};\nКоличество лайков: {df1.iloc[-1].likes};\nКоличество просмотров: {df1.iloc[-1].views};\nЗначение CTR: {df1.iloc[-1].CTR}."
        bot.sendMessage(chat_id = chat_id, text = message)
# Создается f-строка, в которую указываются последние значения по столбцам (за вчерашний день), а также тест для правильного восприятия информации, данную строку бот отправляет в чат как сообщение

    @task()
    def send_four_charts_2_2(df1, chat_id):
        figure = plt.figure()
        figure.set_size_inches(18.5, 10.5, forward=True)
        
        ax1 = figure.add_subplot(2, 2, 1)
        ax2 = figure.add_subplot(2, 2, 2)
        ax3 = figure.add_subplot(2, 2, 3)
        ax4 = figure.add_subplot(2, 2, 4)
        
        ax1.plot(df1['date'], df1['DAU'], color = 'blue')
        ax2.plot(df1['date'], df1['likes'], color = 'blue')
        ax3.plot(df1['date'], df1['views'], color = 'blue')
        ax4.plot(df1['date'], df1['CTR'], color = 'blue')
        
        ax1.set_title('DAU')
        ax2.set_title('Количество лайков')
        ax3.set_title('Количество просмотров')
        ax4.set_title('CTR')
        
        figure.suptitle('Значения метрик за предыдущие 7 дней', fontsize=28)
        
        four_charts = io.BytesIO()
        plt.savefig(four_charts)
        four_charts.seek(0)
        four_charts.name = 'four_charts.png'
        plt.close()
        
        bot.sendPhoto(chat_id = chat_id, photo = four_charts)

 # При помощи бибиотеки matplotlib создается график из четырех графиков продуктовых метрик за последюю неделю, этот график бот отправляет в чат после сообщения    
    
    df1 = extract_1()
    send_message_2_1(df1, chat_id)
    send_four_charts_2_2(df1, chat_id)

i_gromov_dag_task_7_1 = i_gromov_dag_task_7_1()
    

        
        
        
        