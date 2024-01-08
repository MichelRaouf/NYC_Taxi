import os
import pandas as pd
import functions as fn

def extraction_task(filename):
    if not os.path.exists("./data/green_trip_data_2015-1_with_gps.csv"):
        nyc_df = pd.read_csv(filename)
        API_KEY = "AIzaSyDuASYlfRWIV8CsKDo3PRgMCHqYdDM4k2A"
        nyc_df = fn.add_gps_coordinates(nyc_df, API_KEY)
        nyc_df.to_csv("./data/green_trip_data_2015-1_with_gps.csv", index=False)