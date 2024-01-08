import os
import pandas as pd
from sqlalchemy import create_engine

def integration_task(filename_rtl, filename_gps, filename_lookup):
    if not os.path.exists("./data/green_trip_data_2015-1clean.csv"):
        nyc_df = pd.read_csv(filename_rtl)
        gps_df = pd.read_csv(filename_gps)
        lookup_table = pd.read_csv(filename_lookup)
        nyc_df['latitude_pickup'] = gps_df['latitude_pickup']
        nyc_df['longitude_pickup'] = gps_df['longitude_pickup']
        nyc_df['latitude_dropoff'] = gps_df['latitude_dropoff']
        nyc_df['longitude_dropoff'] = gps_df['longitude_dropoff']
        nyc_df.to_csv("./data/green_trip_data_2015-1clean.csv", index=False)
        engine = create_engine('postgresql://root:root@pgdatabase:5432/nyc_green_taxi')
        if (engine.connect()) :
            print('connected succesfully')
        else:
            print('failed to connect')
        try:
            nyc_df.to_sql(name='M4_green_taxis_01_2015', con=engine, if_exists='fail')
            lookup_table.to_sql(name='lookup_table', con=engine, if_exists='fail')
        except ValueError as ve:
            print("Tables are already loaded in the database")