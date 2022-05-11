import pandas as pd
from datetime import datetime as dt
from utils import logger, connect_db, load, select_table_from_db


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