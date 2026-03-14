import os
import pandas as pd

from dotenv import load_dotenv

from data_loader_class import DataLoader
from show_or_describe_data import DataFrameManipulator

load_dotenv()
KEY_FILE = os.environ.get('KEYFILEPATH')

data_loader = DataLoader(KEY_FILE)


# Do wywolania tylko jak nie ma rzeczy juz w data
def load_data_with_data_loader_using_query():
    data_loader.make_a_query_and_save_to_class("""
    SELECT *
    FROM `bigquery-public-data.noaa_gsod.gsod2020`
    LIMIT 1000
    """, 'basic_2020_query')

    data_loader.make_a_query_and_save_to_class("""
        SELECT *
        FROM `bigquery-public-data.noaa_gsod.gsod2021`
        LIMIT 1000
        """, 'basic_2021_query')

    data_loader.make_a_query_and_save_to_class("""
    SELECT *
    FROM bigquery-public-data.noaa_gsod.stations
    LIMIT 1000
    """, 'stations_query')

    data_loader.save_all_df_to_csv()


data_manipulator = DataFrameManipulator()

loaded = data_loader.load_dfs_from_csv_to_class()
print(loaded)

if loaded == 0:
    load_data_with_data_loader_using_query()
    data_loader.save_all_df_to_csv()

# 4.1. Chcemy posiadać podstawowe informacje o lokalizacjach pomiarów pogodowych (stacje) oraz krajach,
# tak aby dane były zrozumiałe dla człowieka i możliwe do dalszego przetwarzania

main_data_2020 = data_loader.get_df_from_class('basic_2020_query')
main_data_2021 = data_loader.get_df_from_class('basic_2021_query')

all_main_data = pd.concat([main_data_2021, main_data_2020], ignore_index=True)

station_main_data = data_loader.get_df_from_class('stations_query')

numeric_columns_for_main = all_main_data.columns[5:]
numeric_columns_for_stations = station_main_data.columns[6:8]

data_manipulator.clear_data_frame(
    all_main_data,
    numeric_columns= numeric_columns_for_main
)
data_manipulator.clear_data_frame(
    station_main_data,
    numeric_columns=numeric_columns_for_stations
)

data_manipulator.change_column_names({'usaf':'stn'}, station_main_data)

inner_joined_station_and_main = data_manipulator.join_two_dfs(
    station_main_data,all_main_data, 'stn', 'inner'
)

columns_needed = ['country', 'state', 'stn','wban_x', 'name']

basic_info_country_loc = data_manipulator.use_only_columns_needed(inner_joined_station_and_main, columns_needed)

print(basic_info_country_loc)

data_loader.save_df_to_csv(basic_info_country_loc, 'data/basic_info_country_loc.csv')


# 4.2. Chcemy wygenerować podstawowe zestawienia dotyczące warunków
# pogodowych na świecie (np. temperatura, opady, wiatr)
# w ujęciu dziennym.

mapping_columns_for_main = {
        'da': 'Day',
        'temp':'Temperature',
        'slp':'Sea Level Pressure',
        'wdsp':'Wind Speed',
        'prcp':'Precipitation',

     }
data_manipulator.change_column_names(mapping_columns_for_main, all_main_data)
columns_needed = ['stn', 'date', 'Temperature', 'max', 'min', 'Wind Speed', 'Precipitation']

daily_weather_summary = data_manipulator.use_only_columns_needed(all_main_data, columns_needed)
daily_weather_report = (
    daily_weather_summary
    .groupby('date')
    .agg({
        'Temperature': 'mean',
        'max': 'max',
        'min': 'min',
        'Wind Speed': 'mean',
        'Precipitation': 'sum'
    })
    .reset_index()
)
print(daily_weather_report.head())

data_loader.save_df_to_csv(daily_weather_report, 'data/daily_weather_report.csv')


# 4.3. Chcemy przygotować dane umożliwiające analizę zmian warunków pogodowych w czasie
# dla wybranych lokalizacji i zmiennych pogodowych.

# Wybrane kraje do analizy
selected_countries = ['NO', 'SV', 'AF']

# Wybór potrzebnych kolumn
time_analysis_columns = [
    'country',
    'name',
    'stn',
    'date',
    'temp',
    'max',
    'min',
    'wdsp',
    'prcp',
]

time_analysis_data = data_manipulator.use_only_columns_needed(inner_joined_station_and_main, time_analysis_columns)

# # Sortowanie danych w czasie
# time_analysis_data = time_analysis_data.sort_values(by=['country', 'stn', 'date'])

# Zapis do CSV
data_loader.save_df_to_csv(time_analysis_data, 'data/weather_time_analysis.csv')

print(time_analysis_data.head())