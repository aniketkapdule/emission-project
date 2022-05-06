import os
from functools import wraps
import pandas as pd
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine, inspect
import shutil
import psycopg2
import string
import random

default_args={
    'owner':'Aniket',
    "depends_on_past":False,
    'start_date':days_ago(0,0,0,0,0),
    'retries':1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'emission_dag',
    default_args=default_args,
    description='emission dag',
    schedule_interval=timedelta(days=1)
)

#data paths
drivers = 'data/drivers.csv'
vehicle_fuel_consumptions = 'data/vehicle_fuel_consumptions.csv'
drivers_logbook = 'data/incoming_data/drivers_logbook.csv'

#just a function for logging in airflow logs
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

    #if table_name in ['drivers', 'cars']:
    #    try:
    #        df.to_sql('drivers', db_engine, if_exists='fail', index=False)
    #    except ValueError:
    #        print('Table already')
    #elif table_name in ['car_driver_logbook']:

def select_table_from_db(table):
    db_engine, cursor = connect_db()
    df = pd.read_sql_query(f"SELECT * from {table};", db_engine)
    print(f"selected {table}")
    return df

@logger
def create_tables():
    db_engine, cursor = connect_db()

    #Creating drivers table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS drivers(
        driver_id SERIAL PRIMARY KEY,
        name VARCHAR(50),
        first_name VARCHAR(50)
    );''')
    print('drivers done')

    #creating cars table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS cars(
        car_id SERIAL PRIMARY KEY,
        brand VARCHAR(50), 
        model VARCHAR(50), 
        vehicle_class VARCHAR(50), 
        engine_size_l FLOAT, 
        cylinders FLOAT, 
        transmission VARCHAR(50), 
        fuel_type VARCHAR(50),
        fuel_consumption_l_per_hundred_km FLOAT,
        hwy_l_per_hundred_km FLOAT,
        comb_l_per_hundred FLOAT,
        comb_mpg INT,
        co2_emission_g_per_km INT
    );''')
    print('car done')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS car_driver_log(
	    car_id INT,
	    driver_id INT,
	    start_city VARCHAR(50),
	    start_country VARCHAR(50),
	    target_city VARCHAR(50),
	    target_country VARCHAR(50),
	    distance_km FLOAT,
	    date DATE,
	    total_emission FLOAT,
	    CONSTRAINT fk__cars__car_id__car_driver_log__car_id
	    FOREIGN KEY (car_id)
	    REFERENCES cars(car_id)
	    ON UPDATE CASCADE ON DELETE RESTRICT,
	    CONSTRAINT fk__drivers__driver_id__car_driver_log__driver_id
	    FOREIGN KEY (driver_id)
	    REFERENCES drivers(driver_id)
	    ON UPDATE CASCADE ON DELETE RESTRICT
    );''')   
    print('car_driver_log done')

@logger
def extract(drivers, vehicle_fuel_consumptions, drivers_logbook):
    print("extracting the data")
    df_drivers_raw = pd.read_csv(drivers)
    df_veh_cons_raw = pd.read_csv(vehicle_fuel_consumptions)
    df_drivers_logbook_raw = pd.read_csv(drivers_logbook)
    #it will check if the data is there or not, if the data is not there it will initiate an empty dataframe.
    #if os.path.isfile(drivers_logbook):
    #    df_drivers_logbook_raw = pd.read_csv(drivers_logbook)
    #    shutil.move("data/incoming_data/drivers_logbook.csv", "data/used_data/drivers_logbook.csv")
    #else:
    #    print('No new data!')
    #    #empty dataframe
    #    df_drivers_logbook_raw = pd.DataFrame(columns=['brand', 'model', 'engine_size_l', 'cylinders', 'fuel_type',
    #   'transmission', 'name', 'first_name', 'start_city', 'start_country',
    #   'target_city', 'target_country', 'distance_km', 'date'])
    return df_drivers_raw, df_veh_cons_raw, df_drivers_logbook_raw           

@logger
def transform_and_load():
    db_engine, cursor = connect_db()
    df_drivers_raw, df_veh_cons_raw, df_drivers_logbook_raw = extract(drivers, vehicle_fuel_consumptions, drivers_logbook)
    print('Fetched the data')
    #driver table transformations
    df_drivers_clean = df_drivers_raw[['name', 'first_name']].sort_values('name').reset_index(drop=True)
    #deduping the data
    df_drivers_clean = df_drivers_clean.drop_duplicates(subset=['name', 'first_name']).reset_index(drop=True)
    df_drivers_count = pd.read_sql_query("SELECT COUNT(driver_id) FROM drivers;", db_engine)
    drivers_count = df_drivers_count.iloc[0].values[0]
    if drivers_count == 0:
        load(df_drivers_clean, table_name='drivers')
    else:
        pass

    #cars table
    df_veh_cons_raw.rename(columns={'BRAND':'brand', 'MODEL':'model', 'VEHICLE CLASS':'vehicle_class', 'ENGINE SIZE L':'engine_size_l', 'CYLINDERS':'cylinders',
       'TRANSMISSION':'transmission', 'FUEL_TYPE':'fuel_type', 'FUEL CONSUMPTION (L/100 km)':'fuel_consumption_l_per_hundred_km',
       'HWY (L/100 km)':'hwy_l_per_hundred_km', 'COMB (L/100 km)':'comb_l_per_hundred', 'COMB (mpg)':'comb_mpg',
       'CO2_Emissions(g/km)':'co2_emission_g_per_km'}, inplace=True)
    df_veh_cons_raw.sort_values('brand', inplace=True)
    df_veh_cons_raw = df_veh_cons_raw.reset_index(drop=True)
    #deduping
    df_cars_clean = df_veh_cons_raw.drop_duplicates(subset=['brand', 'model','vehicle_class', 'engine_size_l', 'cylinders', 'transmission', 'fuel_type']).reset_index(drop=True)
    
    # this logic should only be there if the car and drivers data is static.
    df_cars_count = pd.read_sql_query("SELECT COUNT(car_id) FROM cars;", db_engine)
    cars_count = df_cars_count.iloc[0].values[0]
    if cars_count == 0:
        load(df_cars_clean, table_name='cars')
    else:
        pass
    
    load(df_drivers_logbook_raw, table_name='drivers_logbook_raw')

    df_cars_db = select_table_from_db(table="cars")
    df_drivers_db = select_table_from_db(table="drivers")
    
    # this if condition will check if we have received the new data or not.
    if len(df_drivers_logbook_raw) > 0:
        df_car_log = df_cars_db.merge(df_drivers_logbook_raw, how='right', on=['brand', 'model', 'engine_size_l', 'cylinders', 'fuel_type', 'transmission'])
        # adding a total_emission column
        values = df_car_log.distance_km * df_car_log.co2_emission_g_per_km
        df_car_log['total_emission'] = values
        df_car_driver_log_raw = df_car_log.merge(df_drivers_db, how='left', on=['name', 'first_name'])[['car_id', 'driver_id', 'start_city', 'start_country', 'target_city',
            'target_country', 'distance_km', 'date', 'total_emission']]
        df_cdl_count = pd.read_sql_query("SELECT COUNT(car_id) FROM car_driver_log;", db_engine)
        cdl_count = df_cdl_count.iloc[0].values[0]
        if cdl_count == 0:
            df_car_driver_log_clean = df_car_driver_log_raw.drop_duplicates(subset=['car_id', 'driver_id', 'start_city', 'start_country', 'target_country', 'target_city'])
            load(df_car_driver_log_clean, table_name='car_driver_log')
        else:
            df_car_driver_log_db = select_table_from_db(table='car_driver_log')
            df_car_driver_log_merge = df_car_driver_log_raw.merge(df_car_driver_log_db, on=['car_id', 'driver_id', \
            'start_city', 'start_country', 'target_country', 'target_city'], how='left', indicator=True)
            df_car_driver_log_clean = df_car_driver_log_merge[df_car_driver_log_merge['_merge'] == 'left_only'].drop(['distance_km_y', 'date_y', \
            'total_emission_y', '_merge'], axis=1).rename(columns={'distance_km_x':'distance_km', 'date_x':'date', 'total_emission_x':'total_emission'})
            df_car_driver_log_clean['date']= pd.to_datetime(df_car_driver_log_clean['date']) # making sure the datatype of date column
            load(df_car_driver_log_clean, 'car_driver_log')
            print(df_car_driver_log_clean)
    else:
        print('No new data!')

with dag:
    create_tables_if_not_exists = PythonOperator(
        task_id='creating_tables_if_not_exists',
        python_callable=create_tables
    )
    t_and_l = PythonOperator(
        task_id='Transform_and_load',
        python_callable=transform_and_load
    )

create_tables_if_not_exists >> t_and_l