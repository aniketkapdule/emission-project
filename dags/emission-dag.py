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
from datetime import datetime as dt

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
drivers = 'data/drivers_incoming_data/drivers.csv'
vehicle_fuel_consumptions = 'data/cars_incoming_data/vehicle_fuel_consumptions.csv'
drivers_logbook = 'data/logbook_incoming_data/drivers_logbook.csv'

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

def select_table_from_db(table):
    db_engine, cursor = connect_db()
    df = pd.read_sql_query(f"SELECT * from {table};", db_engine)
    print(f"selected {table}")
    return df

def create_date_dim(start_date='2010-01-01', end_date='2030-01-01'):
    df = pd.DataFrame({"date":pd.date_range(start_date, end_date)})
    df["week_day"] = df.date.dt.day_name()
    df["day"] = df.date.dt.day
    df["month"] = df.date.dt.month
    df["week"] = df.date.dt.isocalendar().week
    df["quarter"] = df.date.dt.quarter
    df["year"] = df.date.dt.year
    df.insert(0, 'date_id', (df.year.astype(str) + df.month.astype(str).str.zfill(2) + df.day.astype(str).str.zfill(2)).astype(int))
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

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS country(
            country_id SERIAL PRIMARY KEY,
            country_name VARCHAR(50)
        );
        """)
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS city(
            city_id SERIAL PRIMARY KEY,
            city_name VARCHAR(50),
            country_id INT,
            CONSTRAINT fk__country__country_id__city__country_id
            FOREIGN KEY (country_id)
            REFERENCES country(country_id)
            ON UPDATE CASCADE ON DELETE CASCADE
        );
        """)

    # Adding date dimension table if not exists
    database_tables = pd.read_sql_query("""SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE';""", db_engine)
    if 'date' in database_tables['table_name'].values:
        pass
    else:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS date(
	            date_id INT PRIMARY KEY,
	            date DATE,
	            week_day VARCHAR(10),
	            day INT,
	            month INT,
	            week INT,
	            quarter INT,
	            year INT
            );
        """)
        load(create_date_dim(), 'date', if_exists='append')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS car_driver_log(
	    car_id INT,
	    driver_id INT,
	    start_city_id INT,
	    start_country_id INT,
	    target_city_id INT,
	    target_country_id INT,
	    distance_km FLOAT,
	    date_id INT,
	    total_emission FLOAT,
	    CONSTRAINT fk__cars__car_id__car_driver_log__car_id
	    FOREIGN KEY (car_id)
	    REFERENCES cars(car_id)
	    ON UPDATE CASCADE ON DELETE RESTRICT,
	    CONSTRAINT fk__drivers__driver_id__car_driver_log__driver_id
	    FOREIGN KEY (driver_id)
	    REFERENCES drivers(driver_id)
	    ON UPDATE CASCADE ON DELETE RESTRICT,
        CONSTRAINT fk__date__date_id__car_driver_log__date_id
	    FOREIGN KEY (date_id)
	    REFERENCES date(date_id)
	    ON UPDATE CASCADE ON DELETE RESTRICT,
		CONSTRAINT fk__city__city_id__car_driver_log__start_city_id
		FOREIGN KEY (start_city_id)
		REFERENCES city(city_id)
		ON UPDATE CASCADE ON DELETE RESTRICT,
		CONSTRAINT fk__city__city_id__car_driver_log__target_city_id
		FOREIGN KEY (target_city_id)
		REFERENCES city(city_id)
		ON UPDATE CASCADE ON DELETE RESTRICT,
		CONSTRAINT fk__country__country_id__car_driver_log__target_country_id
		FOREIGN KEY (target_country_id)
		REFERENCES country(country_id)
		ON UPDATE CASCADE ON DELETE RESTRICT,
		CONSTRAINT fk__country__country_id__car_driver_log__start_country_id
		FOREIGN KEY (start_country_id)
		REFERENCES country(country_id)
		ON UPDATE CASCADE ON DELETE RESTRICT
    );''')   
    print('car_driver_log done')


@logger
def extract(drivers, vehicle_fuel_consumptions, drivers_logbook):
    print("extracting the data")
    timestr = dt.now().strftime("%d_%m_%Y_%H_%M_%S")
    # it will check if the data is there or not, if the data is not there it will initiate an empty dataframe.
    if os.path.isfile(drivers):
        df_drivers_raw = pd.read_csv(drivers)
        shutil.move("data/drivers_incoming_data/drivers.csv", f"data/drivers_used_data/drivers_{timestr}.csv")
    else:
        print('No new drivers data!')
        #empty dataframe
        df_drivers_raw = pd.DataFrame(columns=['name', 'first_name'])
    
    if os.path.isfile(vehicle_fuel_consumptions):
        df_veh_cons_raw = pd.read_csv(vehicle_fuel_consumptions)
        shutil.move("data/cars_incoming_data/vehicle_fuel_consumptions.csv", f"data/cars_used_data/vehicle_fuel_consumptions_{timestr}.csv")
    else:
        print('No new cars data!')
        #empty dataframe
        df_veh_cons_raw = pd.DataFrame(columns=['brand', 'model', 'vehicle_class', 'engine_size_l', 
        'cylinders', 'transmission', 'fuel_type', 'fuel_consumption_l_per_hundred_km', 
        'hwy_l_per_hundred_km', 'comb_l_per_hundred', 'comb_mpg', 'co2_emission_g_per_km'])
    
    if os.path.isfile(drivers_logbook):
        df_drivers_logbook_raw = pd.read_csv(drivers_logbook)
        shutil.move("data/logbook_incoming_data/drivers_logbook.csv", f"data/logbook_used_data/drivers_logbook{timestr}.csv")
    else:
        print('No new logbook data!')
        #empty dataframe
        df_drivers_logbook_raw = pd.DataFrame(columns=['brand', 'model', 'engine_size_l', 'cylinders', 'fuel_type',
       'transmission', 'name', 'first_name', 'start_city', 'start_country',
       'target_city', 'target_country', 'distance_km', 'date'])
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
    
    #load(df_drivers_logbook_raw, table_name='drivers_logbook_raw')
    df_cars_db = select_table_from_db(table="cars")
    df_drivers_db = select_table_from_db(table="drivers")


    
    # this if condition will check if we have received the new data or not.
    if len(df_drivers_logbook_raw) > 0:
        # country table transformation
        df_country_count = pd.read_sql_query("SELECT count(country_id) FROM country;", db_engine)
        country_count = df_country_count.iloc[0].values[0]
        df_s_country_logbook= df_drivers_logbook_raw[['start_country']].drop_duplicates().rename(columns={'start_country':'country_name'}).reset_index(drop=True)
        df_t_country_logbook= df_drivers_logbook_raw[['target_country']].drop_duplicates().rename(columns={'target_country':'country_name'}).reset_index(drop=True)
        if country_count == 0:    
            df_country_clean = pd.concat([df_s_country_logbook, df_t_country_logbook], axis=0).drop_duplicates().reset_index(drop=True)
            load(df_country_clean, 'country')
        else:
            df_country_db = select_table_from_db('country')
            df_country = df_s_country_logbook.merge(df_country_db, how='left', on=['country_name'], indicator=True)
            df_country_clean = df_country[df_country['_merge'] == 'left_only'].drop('_merge', axis=1)
            if len(df_country_clean) > 0:
                load(df_country_clean, 'country')
            else:
                pass
        
        # city table transformation
        df_city_count = pd.read_sql_query("SELECT count(city_id) FROM city;", db_engine)
        city_count = df_city_count.iloc[0].values[0]
        df_s_city_logbook = df_drivers_logbook_raw[['start_city', 'start_country']].drop_duplicates().rename(columns={'start_city':'city_name', 'start_country':'country_name'}).reset_index(drop=True)
        df_t_city_logbook = df_drivers_logbook_raw[['target_city', 'target_country']].drop_duplicates().rename(columns={'target_city':'city_name','target_country':'country_name'}).reset_index(drop=True)
        if city_count==0:
            df_country_db = select_table_from_db('country')
            df_city = pd.concat([df_s_city_logbook, df_t_city_logbook], axis=0).drop_duplicates().reset_index(drop=True)
            df_city_country = df_city.merge(df_country_clean, how='left', on=['country_name'])
            df_city_country_clean = df_city_country.merge(df_country_db, how='left', on=['country_name'])
            df_city_country_clean.drop(columns=['country_name'], inplace=True)
            load(df_city_country_clean, 'city')
        else:
            df_city_db = select_table_from_db('city')
            df_city = pd.concat([df_s_city_logbook, df_t_city_logbook], axis=0).drop_duplicates().reset_index(drop=True)
            df_city_country_merge = df_city.merge(df_city_db, how='left', on=['city_name'], indicator=True)
            df_city_country_clean = df_city_country_merge.loc[df_city_country_merge['_merge']=='left_only'].drop(columns=['_merge', 'country_name'])
            load(df_city_country_clean, 'city')

        #adding car_id in car_driver_log
        df_car_log = df_cars_db.merge(df_drivers_logbook_raw, how='right', on=['brand', 'model', 'engine_size_l', 'cylinders', 'fuel_type', 'transmission'])
        
        # adding a total_emission column
        values = df_car_log.distance_km * df_car_log.co2_emission_g_per_km
        df_car_log['total_emission'] = values

        # adding driver_id in car_driver_log
        df_car_driver_log_raw = df_car_log.merge(df_drivers_db, how='left', on=['name', 'first_name'])[['car_id', 'driver_id', 'start_city', 'start_country', 'target_city',
            'target_country', 'distance_km', 'date', 'total_emission']]
        df_car_driver_log_raw['date'] = pd.to_datetime(df_car_driver_log_raw['date'])
        df_date_db = select_table_from_db('date')
        df_date_db['date'] = pd.to_datetime(df_date_db['date'])

        # adding date_id in car_driver_log
        df_car_driver_date_log_raw = df_car_driver_log_raw.merge(df_date_db, how='left', on=['date'])
        df_car_driver_date_log_raw = df_car_driver_date_log_raw[['car_id', 'driver_id', 'start_city', 'start_country', 'target_city', 'target_country', 'distance_km', 'date_id', 'total_emission']]
        
        #adding city_id in car_driver_log
        df_city_db = select_table_from_db('city')
        df_car_driver_date_start_city_raw = df_car_driver_date_log_raw.merge(df_city_db, how='left',    \
        left_on=['start_city'], right_on=['city_name'])  \
        [['car_id', 'driver_id', 'city_id', 'start_country', 'target_city', 'target_country', 'distance_km', 'date_id', 'total_emission']] \
        .rename(columns={'city_id':'start_city_id'})
        df_car_driver_date_target_city_raw = df_car_driver_date_start_city_raw.merge(df_city_db, how='left',    \
        left_on=['target_city'], right_on=['city_name'])  \
        [['car_id', 'driver_id', 'start_city_id', 'city_id', 'start_country', 'target_country', 'distance_km', 'date_id', 'total_emission']] \
        .rename(columns={'city_id':'target_city_id'})

        #adding country_id in car_driver_log
        df_country_db = select_table_from_db('country')
        df_car_driver_date_start_city_raw = df_car_driver_date_target_city_raw.merge(df_country_db, how='left',    \
        left_on=['start_country'], right_on=['country_name'])  \
        [['car_id', 'driver_id', 'start_city_id', 'target_city_id', 'country_id', 'target_country', 'distance_km', 'date_id', 'total_emission']] \
        .rename(columns={'country_id':'start_country_id'})
        df_car_driver_date_city_country_raw = df_car_driver_date_start_city_raw.merge(df_country_db, how='left',    \
        left_on=['target_country'], right_on=['country_name'])  \
        [['car_id', 'driver_id', 'start_city_id', 'target_city_id', 'start_country_id', 'country_id', 'distance_km', 'date_id', 'total_emission']] \
        .rename(columns={'country_id':'target_country_id'})        


        df_cdl_count = pd.read_sql_query("SELECT COUNT(car_id) FROM car_driver_log;", db_engine)
        cdl_count = df_cdl_count.iloc[0].values[0]
        if cdl_count == 0:
            df_car_driver_date_city_country_clean = df_car_driver_date_city_country_raw.drop_duplicates(subset=['car_id', 'driver_id', 'start_city_id', 'start_country_id', 'target_country_id', 'target_city_id', 'date_id'])
            load(df_car_driver_date_city_country_clean, table_name='car_driver_log')
        else:
            df_car_driver_log_db = select_table_from_db(table='car_driver_log')
            df_car_driver_date_city_country_merge = df_car_driver_date_city_country_raw.merge(df_car_driver_log_db, on=['car_id', 'driver_id', \
            'start_city_id', 'start_country_id', 'target_country_id', 'target_city_id', 'date_id'], how='left', indicator=True)
            df_car_driver_date_city_country_clean = df_car_driver_date_city_country_merge[df_car_driver_date_city_country_merge['_merge'] == 'left_only'].drop(['distance_km_y', \
            'total_emission_y', '_merge'], axis=1).rename(columns={'distance_km_x':'distance_km', 'total_emission_x':'total_emission'})
            load(df_car_driver_date_city_country_clean, 'car_driver_log')
            print(df_car_driver_date_city_country_clean)
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