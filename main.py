import os
import pandas as pd

from dotenv import load_dotenv

from data_loader_class import DataLoader
from data_frame_manipulator_class import DataFrameManipulator

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
print(f'Number of loaded data from files: {loaded}')

if loaded == 0:
    print(f'Loading data from server...')
    load_data_with_data_loader_using_query()
    data_loader.save_all_df_to_csv()
    print(f'Loading data from server done and saved to query folder')


main_data_2020 = data_loader.get_df_from_class('basic_2020_query')
main_data_2021 = data_loader.get_df_from_class('basic_2021_query')

all_main_data = pd.concat([main_data_2021, main_data_2020], ignore_index=True)

station_main_data = data_loader.get_df_from_class('stations_query')

text_columns_for_main = [
    'stn', 'wban', 'flag_max', 'flag_min', 'flag_prcp'
]

date_columns_for_main = [
    'date'
]

numeric_columns_for_main = [
    'year', 'mo', 'da',
    'temp', 'count_temp',
    'dewp', 'count_dewp',
    'slp', 'count_slp',
    'stp', 'count_stp',
    'visib', 'count_visib',
    'wdsp', 'count_wdsp',
    'mxpsd', 'gust',
    'max', 'min',
    'prcp', 'sndp',
    'fog', 'rain_drizzle', 'snow_ice_pellets',
    'hail', 'thunder', 'tornado_funnel_cloud'
]

duplicate_columns_for_main = [
    'stn', 'wban', 'date'
]

data_manipulator.clear_data_frame(
    all_main_data,
    numeric_columns_for_main,
    text_columns_for_main,
    date_columns_for_main,
    duplicate_columns_for_main
)

text_columns_for_station = [
    'usaf', 'wban', 'name',
    'country', 'state','call'
]

date_columns_for_station = [
    'begin', 'end'
]

numeric_columns_for_station = [
    'lat', 'lon', 'elev'
]

duplicate_columns_stations = [
    'usaf', 'wban'
]

data_manipulator.clear_data_frame(
    station_main_data,
    numeric_columns_for_station,
    text_columns_for_station,
    date_columns_for_station,
    duplicate_columns_stations
)

data_manipulator.change_column_names(
    {'usaf':'stn'},
    station_main_data
)

inner_joined_station_and_main = data_manipulator.join_two_dfs(
    station_main_data,
    all_main_data,
    ['stn', 'wban'],
    'inner'
)

# To juz w sumie zbior pod pkt 5 bo dalsze dzialania
# to juz jakies wycinki tych samych danych ale pod pkt 5 to
# wlasnie bedzie calym zbiorem
data_loader.save_df_to_csv(
    inner_joined_station_and_main,
    'data/report/inner_joined_station_and_main.csv'
)

# 4.1. Chcemy posiadać podstawowe informacje o lokalizacjach pomiarów
# pogodowych (stacje) oraz krajach,tak aby dane były zrozumiałe dla
# człowieka i możliwe do dalszego przetwarzania

columns_needed = [
    'country', 'state', 'stn', 'wban', 'name',
    'lat', 'lon', 'elev', 'begin', 'end'
]

basic_info_country_loc = data_manipulator.use_only_columns_needed(
    inner_joined_station_and_main,
    columns_needed
)

data_loader.save_df_to_csv(
    basic_info_country_loc,
    'data/report/basic_info_country_loc.csv'
)


# 4.2. Chcemy wygenerować podstawowe zestawienia dotyczące warunków
# pogodowych na świecie (np. temperatura, opady, wiatr)
# w ujęciu dziennym.

columns_needed = [
    'country', 'name', 'stn',
    'wban', 'date', 'temp',
    'max', 'min', 'wdsp',
    'prcp'
]
daily_weather_data = data_manipulator.use_only_columns_needed(
    inner_joined_station_and_main,
    columns_needed
)

data_loader.save_df_to_csv(
    daily_weather_data,
    'data/report/daily_weather_report.csv'
)

# 4.3. Chcemy poznać zjawiska ekstremalne w danych pogodowych poprzez
# uwypuklenie skrajnych wartości (np. bardzo wysokie/niskie temperatury,
# intensywne opady, silny wiatr).


extreme_weather_columns = [
    'country', 'name', 'stn', 'wban', 'date',
    'temp', 'max', 'flag_max', 'min', 'flag_min',
    'wdsp', 'gust', 'prcp', 'flag_prcp',
    'fog', 'rain_drizzle', 'snow_ice_pellets',
    'hail', 'thunder', 'tornado_funnel_cloud'
]

extreme_weather_data = data_manipulator.use_only_columns_needed(
    inner_joined_station_and_main,
    extreme_weather_columns
)

data_loader.save_df_to_csv(
    extreme_weather_data,
    'data/report/extreme_weather_data.csv'
)


# 4.4. Chcemy przygotować dane umożliwiające analizę zmian warunków pogodowych w czasie
# dla wybranych lokalizacji i zmiennych pogodowych.

# Wybrane kraje do analizy
selected_countries = [
    'NO', 'SV', 'AF'
]

# Wybór potrzebnych kolumn
time_analysis_columns = [
    'country', 'name', 'stn', 'date',
    'temp', 'max', 'min','wdsp',
    'prcp'
]

time_analysis_data = data_manipulator.use_only_columns_needed(
    inner_joined_station_and_main,
    time_analysis_columns
)

time_analysis_data = time_analysis_data[
    time_analysis_data['country'].isin(selected_countries)
]

# # Sortowanie danych w czasie
time_analysis_data = time_analysis_data.sort_values(
    by=['country', 'stn', 'date']
)

# Zapis do CSV
data_loader.save_df_to_csv(
    time_analysis_data,
    'data/report/weather_time_analysis.csv'
)

#4.5 dane do analizy sezonowosci

seasonality_columns = [
    'country', 'name', 'stn', 'wban', 'date',
    'year', 'mo', 'temp', 'prcp', 'wdsp'
]

seasonality_data = data_manipulator.use_only_columns_needed(
    inner_joined_station_and_main,
    seasonality_columns
)

data_loader.save_df_to_csv(
    seasonality_data,
    'data/report/weather_seasonality_data.csv')