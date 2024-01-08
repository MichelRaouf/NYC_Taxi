import functions as fn
import pandas as pd
import os

def rtl_task(filename):
    if not os.path.exists("./data/green_trip_data_2015-1_rtl.csv"):
        nyc_df = fn.load_dataset(filename)
        fn.set_float_format()
        columns = ['Column name', 'Original value', 'Imputed value']
        lookup_table = pd.DataFrame(columns=columns)
        fn.rename_columns(nyc_df)
        fn.drop_duplicates(nyc_df)
        fn.filter_pickup(nyc_df, '01')
        negative_rows = fn.get_rows_with_negatives(nyc_df)
        converted_negatives = negative_rows.apply(lambda x: x.map(fn.convert_to_positive))
        columns_for_comparison = ['vendor', 'pickup_datetime', 'dropoff_datetime', 'store_and_fwd_flag',
                            'rate_type', 'pickup_location', 'dropoff_location', 'passenger_count',
                            'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount',
                            'tolls_amount', 'improvement_surcharge', 'total_amount']
        problematic_rows = fn.filter_matching_rows(converted_negatives, nyc_df, columns_for_comparison)
        fn.drop_records_in_second_df(problematic_rows, nyc_df)
        negative_rows_2 = fn.get_rows_with_negatives(nyc_df)
        fn.drop_records_in_second_df_2(negative_rows_2, nyc_df)
        rows_invalid_extra = fn.get_rows_with_invalid_extra(nyc_df)
        fn.drop_records_in_second_df_2(rows_invalid_extra, nyc_df)
        rows_invalid_mta = fn.get_rows_with_invalid_mta_tax(nyc_df)
        fn.drop_records_in_second_df_2(rows_invalid_mta, nyc_df)
        fn.modify_passenger_count(nyc_df, 555, 5)
        fn.modify_payment_type(nyc_df)
        lookup_table = fn.imp_ehail_fee(nyc_df, lookup_table)
        lookup_table = fn.imp_congestion_surcharge(nyc_df, lookup_table)
        wrong_rows = fn.mismatch_rows(nyc_df, ['fare_amount','extra','mta_tax','tip_amount','tolls_amount','ehail_fee','improvement_surcharge'], 'total_amount')
        fn.drop_records_in_second_df_2(wrong_rows, nyc_df)
        lookup_table = fn.imp_extra(nyc_df, lookup_table)
        lookup_table = fn.impute_payment_type(nyc_df, lookup_table)
        lower_trip_distance, upper_trip_distance = fn.get_bounds(nyc_df, 'trip_distance')
        fn.impute_outliers(nyc_df, 'trip_distance', lower_trip_distance, upper_trip_distance)
        lower_fare_amount, upper_fare_amount = fn.get_bounds(nyc_df, 'fare_amount')
        fn.impute_outliers(nyc_df, 'fare_amount', lower_fare_amount, upper_fare_amount)
        lower_tip_amount, upper_tip_amount = fn.get_bounds(nyc_df, 'tip_amount')
        fn.impute_outliers(nyc_df, 'tip_amount', lower_tip_amount, upper_tip_amount)
        lower_total_amount, upper_total_amount = fn.get_bounds(nyc_df, 'total_amount')
        fn.impute_outliers(nyc_df, 'total_amount', lower_total_amount, upper_total_amount)
        fn.convert_to_datetime(nyc_df, 'pickup_datetime')
        fn.convert_to_datetime(nyc_df, 'dropoff_datetime')
        fn.add_weeks_columns(nyc_df, 'pickup_datetime')
        nyc_df.to_csv("./data/green_trip_data_2015-1_before_encoding.csv")
        fn.one_hot_encode_column(nyc_df, 'vendor')
        fn.one_hot_encode_column(nyc_df, 'store_and_fwd_flag')
        fn.one_hot_encode_column(nyc_df, 'rate_type')
        fn.one_hot_encode_column(nyc_df, 'payment_type')
        fn.one_hot_encode_column(nyc_df, 'trip_type')
        lookup_table = fn.label_encode_column(nyc_df, 'pickup_location', lookup_table)
        lookup_table = fn.label_encode_column(nyc_df, 'dropoff_location', lookup_table)
        fn.add_cost_per_mile(nyc_df)
        fn.add_trip_duration(nyc_df)
        fn.add_trip_speed(nyc_df)
        fn.convert_to_csv(lookup_table, './data/lookup_table_green_taxis')
        fn.convert_to_csv(nyc_df, './data/green_trip_data_2015-1_rtl')