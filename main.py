import os
import pandas as pd

from dotenv import load_dotenv

import matplotlib.pyplot as plt

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

data_loader.save_df_to_csv(
    inner_joined_station_and_main,
    'data/report/inner_joined_station_and_main.csv'
)

# ----------------------------------------------------------

# ------------- Part 1 -------------------------------------

# ===========================
# 1. Temperature analysis
# ===========================
fig, ax = plt.subplots(1, 3, figsize=(18, 5))

inner_joined_station_and_main.boxplot(
    ax=ax[0],
    column='temp',
    by='year'
)
ax[0].set_title('Average temperature by year')
ax[0].set_ylabel('Temperature [F]')

inner_joined_station_and_main.boxplot(
    ax=ax[1],
    column='max',
    by='year'
)
ax[1].set_title('Maximum temperature by year')
ax[1].set_ylabel('Temperature [F]')

inner_joined_station_and_main.boxplot(
    ax=ax[2],
    column='min',
    by='year'
)
ax[2].set_title('Minimum temperature by year')
ax[2].set_ylabel('Temperature [F]')

plt.suptitle('Temperature analysis')
plt.tight_layout()
plt.show()


# ===========================
# 2. Precipitation and visibility analysis
# ===========================
fig, ax = plt.subplots(1, 2, figsize=(12, 5))

inner_joined_station_and_main.boxplot(
    ax=ax[0],
    column='prcp',
    by='year'
)
ax[0].set_title('Precipitation by year')
ax[0].set_ylabel('Precipitation [inch]')

inner_joined_station_and_main.boxplot(
    ax=ax[1],
    column='visib',
    by='year'
)
ax[1].set_title('Visibility by year')
ax[1].set_ylabel('Visibility [mile]')

plt.suptitle('Precipitation and visibility analysis')
plt.tight_layout()
plt.show()


# ===========================
# 3. Wind analysis
# ===========================
fig, ax = plt.subplots(1, 3, figsize=(18, 5))

inner_joined_station_and_main.boxplot(
    ax=ax[0],
    column='wdsp',
    by='year'
)
ax[0].set_title('Average wind speed by year')
ax[0].set_ylabel('Wind speed [knot]')

inner_joined_station_and_main.boxplot(
    ax=ax[1],
    column='gust',
    by='year'
)
ax[1].set_title('Maximum wind gust by year')
ax[1].set_ylabel('Wind speed [knot]')

inner_joined_station_and_main.boxplot(
    ax=ax[2],
    column='mxpsd',
    by='year'
)
ax[2].set_title('Maximum sustained wind speed by year')
ax[2].set_ylabel('Wind speed [knot]')

plt.suptitle('Wind analysis')
plt.tight_layout()
plt.show()


# ===========================
# 4. Pressure analysis
# ===========================
fig, ax = plt.subplots(1, 2, figsize=(12, 5))

inner_joined_station_and_main.boxplot(
    ax=ax[0],
    column='slp',
    by='year'
)
ax[0].set_title('Sea level pressure by year')
ax[0].set_ylabel('Pressure [kPa]')

inner_joined_station_and_main.boxplot(
    ax=ax[1],
    column='stp',
    by='year'
)
ax[1].set_title('Station level pressure by year')
ax[1].set_ylabel('Pressure [kPa]')

plt.suptitle('Pressure analysis')
plt.tight_layout()
plt.show()

# ----------------------------------------------------------