import os

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
    LIMIT 10
    """)

    data_loader.make_a_query_and_save_to_class("""
    SELECT *
    FROM bigquery-public-data.noaa_gsod.stations
    LIMIT 10
    """)

    query_names = [
        'basic_query',
        'stations_query'
    ]

    data_loader.save_all_df_to_csv_with_names(query_names)


data_manipulator = DataFrameManipulator()

loaded = data_loader.load_dfs_from_csv_to_class()

if loaded == 0:
    load_data_with_data_loader_using_query()
    data_loader.save_all_df_to_csv()

# 4.1. Chcemy posiadać podstawowe informacje o lokalizacjach pomiarów pogodowych (stacje) oraz krajach,
# tak aby dane były zrozumiałe dla człowieka i możliwe do dalszego przetwarzania

all_main_data = data_loader.get_df_from_class('basic_query')

station_main_data = data_loader.get_df_from_class('stations_query')

data_manipulator.change_column_names({'usaf':'stn'}, station_main_data)

left_joined_station_and_main = data_manipulator.join_two_dfs(
    station_main_data,all_main_data, 'stn'
)

columns_needed = ['country', 'state', 'stn','wban_x', 'name']

print(data_manipulator.use_only_columns_needed(left_joined_station_and_main, columns_needed))

# 4.2. Chcemy wygenerować podstawowe zestawienia dotyczące warunków
# pogodowych na świecie (np. temperatura, opady, wiatr)
# w ujęciu dziennym.

col
