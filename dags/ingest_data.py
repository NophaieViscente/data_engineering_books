from airflow.models import DAG
from datetime import datetime, timezone
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
import pandas as pd

POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = 'docker-postgres'
POSTGRES_DB = 'books'

# Args to DAG   
today = datetime.now()
default_args = {
        'start_date': today,
        'schedule_interval': '0 12 * * SUN',
        'catchup': False,
}

def connect_to_database () :
    print('Connecting to database!')
    connection_str = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:15432/{POSTGRES_DB}"

    engine = create_engine(connection_str)
    if engine.connect() :
        print('Connected!')
    print('Error of connection!')
    return engine

def connect_and_create_table () :
    engine = connect_to_database()
    engine.execute(
        '''
            CREATE TABLE books(
                id INT NOT NULL,
                genre VARCHAR(255),
                names VARCHAR(255),
                prices VARCHAR(255),
                ratings VARCHAR(255),
                stock_status VARCHAR(255)
            );
        ''')

def ingest_data () :
    engine = connect_to_database()
    df_tmp = pd.read_csv(
        f"{today.strftime('%y')}.{today.strftime('%m')}.{today.strftime('%d')}_dataframe_books.csv")
    name_table = 'books'
    df_tmp.to_sql(name_table,engine, if_exists='append')


with DAG(dag_id='connect_database', default_args=default_args) as dag :

    connect_create_table = PythonOperator(
        task_id = 'connect',
        python_callable= connect_and_create_table
    )
    saving_at_database = PythonOperator(
        task_id = 'saving_at_database',
        python_callable=ingest_data
    )

connect_create_table >>saving_at_database