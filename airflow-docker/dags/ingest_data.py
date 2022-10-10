from airflow.models import DAG
from datetime import datetime, timezone
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine, inspect
import json

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
    connection_str = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@database_etl:5432/{POSTGRES_DB}"

    engine = create_engine(connection_str)
    if engine.connect() :
        print('Connected!')
    print('Error of connection!')
    return engine

def save_data () :
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

with DAG(dag_id='connect_database', default_args=default_args) as dag :

    connecting_database = PythonOperator(
        task_id = 'connecting_database',
        python_callable=connect_to_database
    )
    saving_at_database = PythonOperator(
        task_id = 'saving_at_database',
        python_callable=save_data
    )
saving_at_database