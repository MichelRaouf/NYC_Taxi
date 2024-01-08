import pandas as pd
import os
from googlemaps import Client as GoogleMaps

def load_dataset(path):
    return pd.read_csv(path)

def set_float_format():
    pd.set_option('display.float_format', lambda x: '%.3f' % x)

def copy_df(df) :
    return df.copy()

def rename_columns(df):
    df.columns = df.columns.str.lower()
    df.columns = [col.replace('lpep ', '') for col in df.columns]
    df.columns = [col.replace('pu ', 'pickup ') for col in df.columns]
    df.columns = [col.replace('do ', 'dropoff ') for col in df.columns]
    df.columns = [col.replace(' ', '_') for col in df.columns]

def drop_duplicates(df):
    df.drop_duplicates(inplace=True)

def filter_pickup(df, target_month):
    mask = df['pickup_datetime'].str.split(' ').str[0].str.split('-').str[1] == target_month
    df.drop(df[~mask].index, inplace=True)

def get_rows_with_negatives(df):
    filtered_df = df[
        (df.trip_distance < 0) |
        (df.fare_amount < 0) |
        (df.extra < 0) |
        (df.mta_tax < 0) |
        (df.improvement_surcharge < 0) |
        (df.ehail_fee < 0) |
        (df.tip_amount < 0) |
        (df.tolls_amount < 0) |
        (df.total_amount < 0) |
        (df.congestion_surcharge < 0)
    ]
    return filtered_df

def convert_to_positive(value):
    try:
        return abs(float(value))
    except (ValueError, TypeError):
        return value

def filter_matching_rows(df1, df2, columns_for_comparison):
    columns_for_comparison = [col.strip() for col in columns_for_comparison]    
    filtered_df2 = df2[df2[columns_for_comparison].apply(tuple, axis=1).isin(df1[columns_for_comparison].apply(tuple, axis=1))]
    return filtered_df2

def drop_records_in_second_df(df1, df2):
    indices_to_drop = df1.index - 1    
    df2.drop(indices_to_drop, inplace=True)

def drop_records_in_second_df_2(df1, df2):
    indices_to_drop = df1.index    
    df2.drop(indices_to_drop, inplace=True)

def get_rows_with_invalid_extra(df):
    filtered_df = df[(df['extra'] != 0.5) & (df['extra'] != 1.0) & (df['extra'].notna())]
    return filtered_df

def get_rows_with_invalid_mta_tax(df):
    filtered_df = df[(df['mta_tax'] != 0.5) & (df['mta_tax'] != 0.0) & (df['mta_tax'].notna())]
    return filtered_df

def modify_passenger_count(df, old_value, new_value):
    for index, row in df.iterrows():
        if row['passenger_count'] == old_value:
            df.at[index, 'passenger_count'] = new_value

def modify_payment_type(df):
    for index, row in df.iterrows():
        if row['tip_amount'] > 0 & (row['payment_type'] != 'Credit card'):
            df.at[index, 'payment_type'] = 'Credit card'

def imp_ehail_fee(df, lookup_table):
    df['ehail_fee'] = df['ehail_fee'].fillna(0)
    new_row = pd.DataFrame({'Column name': ['ehail_fee'], 
                            'Original value': ['NaN'], 
                            'Imputed value': [0]})
    lookup_table = pd.concat([lookup_table, new_row], ignore_index=True)
    return lookup_table

def imp_congestion_surcharge(df, lookup_table):
    df['congestion_surcharge'] = df['congestion_surcharge'].fillna(0)
    new_row = pd.DataFrame({'Column name': ['congestion_surcharge'], 
                            'Original value': ['NaN'], 
                            'Imputed value': [0]})
    lookup_table = pd.concat([lookup_table, new_row], ignore_index=True)
    return lookup_table

def mismatch_rows(df, columns_to_sum, total_column, tolerance=1e-6):
    row_sums = df[columns_to_sum].sum(axis=1)    
    mismatch_rows = df[abs(row_sums - df[total_column]) > tolerance]
    return mismatch_rows

def imp_extra(df, lookup_table):
    df['extra'] = df['extra'].fillna(0)
    new_row = pd.DataFrame({'Column name': ['extra'], 
                            'Original value': ['NaN'], 
                            'Imputed value': [0]})
    lookup_table = pd.concat([lookup_table, new_row], ignore_index=True)
    return lookup_table

def get_rows_with_null_payment_type(df):
    filtered_df = df[df['payment_type'].isnull()]
    return filtered_df

def impute_payment_type(df, lookup_table):
    payment_type_mode = df['payment_type'].mode()[0]
    df['payment_type'] = df.apply(lambda row: payment_type_mode if (row['tip_amount'] == 0) and pd.isnull(row['payment_type']) else 'Credit Card', axis=1)
    new_row_1 = pd.DataFrame({'Column name': ['payment_type'], 
                            'Original value': ['NaN & tip_amount > 0'], 
                            'Imputed value': ['Credit card']})
    new_row_2 = pd.DataFrame({'Column name': ['payment_type'], 
                            'Original value': ['NaN & tip_amount == 0'], 
                            'Imputed value': [payment_type_mode]})
    lookup_table = pd.concat([lookup_table, new_row_1], ignore_index=True)
    lookup_table = pd.concat([lookup_table, new_row_2], ignore_index=True)
    return lookup_table

def get_bounds(df, column_name):
    Q1 = df[column_name].quantile(0.25)
    Q3 = df[column_name].quantile(0.75)
    IQR = Q3 - Q1
    cut_off = IQR * 1.5
    lower = Q1 - cut_off
    upper = Q3 + cut_off
    return lower, upper

def impute_outliers(df, column_name, lower_bound, upper_bound):
    outliers_mask = (df[column_name] < lower_bound) | (df[column_name] > upper_bound)
    df.loc[outliers_mask, column_name] = pd.NA
    non_outlier_mean = df.loc[~outliers_mask, column_name].mean()
    df[column_name].fillna(non_outlier_mean, inplace=True)

def convert_to_datetime(dataframe, date_column_name):
    dataframe[date_column_name] = pd.to_datetime(dataframe[date_column_name])

def add_weeks_columns(df, column_name):
    df['week_number'] = df[column_name].dt.day // 7 + 1
    week_start = df[column_name] - pd.to_timedelta(df[column_name].dt.dayofweek, unit='D')
    week_end = week_start + pd.DateOffset(days=6)
    df['week_range'] = week_start.astype(str) + ', ' + week_end.astype(str)

def get_gps_coordinates(city, API_KEY):
    gmaps = GoogleMaps(API_KEY)
    result = gmaps.geocode(city)    
    if result and 'geometry' in result[0] and 'location' in result[0]['geometry']:
        location = result[0]['geometry']['location']
        return location['lat'], location['lng']
    else:
        return None, None

def add_gps_coordinates(dataframe, API_KEY):
    if not os.path.exists('./data/gps_coordinates.csv'):
        unique_cities_pickup = dataframe['pickup_location'].unique()
        unique_cities_dropoff = dataframe['dropoff_location'].unique()
        unique_cities = pd.Series(pd.concat([pd.Series(unique_cities_pickup), pd.Series(unique_cities_dropoff)]).unique())
        gps_data = []
        for city in unique_cities:
            latitude, longitude = get_gps_coordinates(city, API_KEY)
            gps_data.append({'pickup_location': city, 'Latitude': latitude, 'Longitude': longitude})
        gps_coordinates = pd.DataFrame(gps_data)
        gps_coordinates.to_csv('./data/gps_coordinates.csv', index=False)
    gps_coordinates = pd.read_csv('./data/gps_coordinates.csv')
    gps_coordinates.rename(columns={'pickup_location': 'location'}, inplace=True)
    merged_dataframe = pd.merge(dataframe, gps_coordinates, left_on='pickup_location', right_on='location', how='left', suffixes=('_pickup', '_dropoff'))
    merged_dataframe.rename(columns={'Latitude': 'latitude_pickup', 'Longitude': 'longitude_pickup'}, inplace=True)    
    merged_dataframe = pd.merge(merged_dataframe, gps_coordinates, left_on='dropoff_location', right_on='location', how='left', suffixes=('_pickup', '_dropoff'))
    merged_dataframe.rename(columns={'Latitude': 'latitude_dropoff', 'Longitude': 'longitude_dropoff'}, inplace=True)
    merged_dataframe.drop(columns=['location_pickup', 'location_dropoff'], inplace=True)
    return merged_dataframe

def one_hot_encode_column(dataframe, column_name):
    categories = dataframe[column_name].unique()    
    for category in categories:
        new_column_name = f"{column_name}_{category}"
        dataframe[new_column_name] = (dataframe[column_name] == category).astype(int)    
    dataframe.drop(columns=[column_name], inplace=True)

def label_encode_column(df, column_name, lookup_table):
    label_mapping = {}
    unique_values = df[column_name].unique()    
    for idx, value in enumerate(unique_values, start=1):
        label_mapping[value] = idx
        new_row = pd.DataFrame({'Column name': [column_name], 
                            'Original value': [value], 
                            'Imputed value': [idx]})
        lookup_table = pd.concat([lookup_table, new_row], ignore_index=True)
    df[column_name] = df[column_name].map(label_mapping)
    return lookup_table

def add_cost_per_mile(df):
    df['cost_per_mile'] = df['fare_amount'] / df['trip_distance']

def add_trip_duration(df):
    df['trip_duration'] = (df['dropoff_datetime'] - df['pickup_datetime']).dt.total_seconds() / 60

def add_trip_speed(df):
    df['trip_speed'] = df['trip_distance'] / df['trip_duration']

def convert_to_csv(df, name):
    df.to_csv(name + '.csv', index=False)

def convert_to_parquet(df, name):
    df.to_parquet(name + '.parquet', index=False)