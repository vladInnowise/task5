from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import re


default_args = {
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'owner': 'admin',
    'retries': 1,
    'retries_delay': timedelta(minutes=1)
}

dag = DAG('task5', default_args=default_args, schedule_interval='@once')


def clean_data():
    path = 'tiktok.csv'
    df = pd.read_csv(path)
    df.dropna(axis=0, how='any', inplace=True)
    df.fillna("-", inplace=True)
    df['at'] = pd.to_datetime(df['at'])
    df = df.sort_values(by='at')
    EMOJI_PATTERN = re.compile(
        "["
        "\U0001F1E0-\U0001F1FF"  # flags (iOS)
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F700-\U0001F77F"  # alchemical symbols
        "\U0001F780-\U0001F7FF"  # Geometric Shapes Extended
        "\U0001F800-\U0001F8FF"  # Supplemental Arrows-C
        "\U0001F900-\U0001F9FF"  # Supplemental Symbols and Pictographs
        "\U0001FA00-\U0001FA6F"  # Chess Symbols
        "\U0001FA70-\U0001FAFF"  # Symbols and Pictographs Extended-A
        "\U00002702-\U000027B0"  # Dingbats
        "\U000024C2-\U0001F251"
        "]+"
    )
    df['content'].replace(EMOJI_PATTERN, '', inplace=True)
    df.to_csv('done_data.csv')


def upload_data():
    df = pd.read_csv('done_data.csv')
    df.to_csv("blin.csv")
    print('all done')

t1 = PythonOperator(task_id="load_variables", python_callable=clean_data, dag=dag)

t2 = PythonOperator(task_id="unload", python_callable=upload_data, dag=dag)

t1 >> t2
