from functools import wraps
import pandas as pd
from sqlalchemy import create_engine, inspect
import psycopg2


def logger(func):
    from datetime import datetime, timezone

    @wraps(func)
    def wrapper(*args, **kwargs):
        called_at = datetime.now(timezone.utc)
        print(f">>> Running {func.__name__!r} function. Logged at {called_at}")
        to_execute = func(*args, **kwargs)
        print(f">>> Function: {func.__name__!r} executed. Logged at {called_at}")
        return to_execute

    return wrapper

@logger
def connect_db():
    print('Connecting to db')
    connection_uri = "postgres+psycopg2://postgres:postgres@database:5432/planetly"
    engine = create_engine(connection_uri, pool_pre_ping=True)
    engine.connect()
    conn = psycopg2.connect(database="planetly", user='postgres',password='postgres', host='database', port='5432')
    conn.autocommit = True
    cursor = conn.cursor()
    return engine, cursor

def load(df, table_name, if_exists='append'): 
    db_engine, cursor = connect_db()
    df.to_sql(table_name, db_engine, if_exists=if_exists, index=False)
    print(f'loaded {table_name}')

def select_table_from_db(table):
    db_engine, cursor = connect_db()
    df = pd.read_sql_query(f"SELECT * from {table};", db_engine)
    print(f"selected {table}")
    return df

