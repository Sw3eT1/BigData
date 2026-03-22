import os
import pandas as pd

from dotenv import load_dotenv

import matplotlib.pyplot as plt
import numpy as np

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
    LIMIT 10000
    """, 'basic_2020_query')

    data_loader.make_a_query_and_save_to_class("""
        SELECT *
        FROM `bigquery-public-data.noaa_gsod.gsod2021`
        LIMIT 10000
        """, 'basic_2021_query')

    data_loader.make_a_query_and_save_to_class("""
    SELECT *
    FROM bigquery-public-data.noaa_gsod.stations
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

data_manipulator.clear_data_frame(
    all_main_data,
    None,
    None,
    None,
    ['stn', 'wban']
)

station_main_data = data_loader.get_df_from_class('stations_query')


data_manipulator.change_column_names(
    {'usaf':'stn'},
    station_main_data
)

data_manipulator.clear_data_frame(
    station_main_data,
    None,
    None,
    None,
    ['stn', 'wban']
)

inner_joined_station_and_main = data_manipulator.join_two_dfs(
    station_main_data,
    all_main_data,
    ['stn', 'wban'],
    'inner'
)

data_loader.save_df_to_csv(
    inner_joined_station_and_main,
    'data/report/inner_joined_station_and_main_dirty.csv'
)

# # ----------------------------------------------------------
#
# # ------------- Part 1 -------------------------------------
#
# # ===========================
# # 1. Temperature analysis
# # ===========================
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
#
#
# # ===========================
# # 2. Precipitation and visibility analysis
# # ===========================
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
#
#
# # ===========================
# # 3. Wind analysis
# # ===========================
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
#
#
# # ===========================
# # 4. Pressure analysis
# # ===========================
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
#
# # ----------------------------------------------------------
#
#
# # ------------- Part 3 -------------------------------------

# # ===========================
# # 3.1 Counting missing values
# # ===========================
missing_values_report = data_manipulator.count_missing_values(
    inner_joined_station_and_main
)

data_loader.save_df_to_csv(
    missing_values_report,
    'data/report/missing_values_report.csv'
)

# -----------------------------

# # ===========================
# # 3.2 Cleaning data with pd.NA
# # ===========================

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

data_manipulator.clear_data_frame(
    all_main_data,
    numeric_columns_for_main,
    text_columns_for_main,
    date_columns_for_main,
    None
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

data_manipulator.clear_data_frame(
    station_main_data,
    numeric_columns_for_station,
    text_columns_for_station,
    date_columns_for_station,
    None
)

inner_joined_station_and_main = data_manipulator.join_two_dfs(
    station_main_data,
    all_main_data,
    ['stn', 'wban'],
    'inner'
)

data_loader.save_df_to_csv(
    inner_joined_station_and_main,
    'data/report/inner_joined_station_and_main_clean.csv'
)
# -----------------------------

# # ===========================
# # 3.3 Putting synthetic values for stp and slp
# # ===========================
mean_tryout = inner_joined_station_and_main.copy(True)
median_tryout = inner_joined_station_and_main.copy(True)
linear_interpolation_tryout = inner_joined_station_and_main.copy(True)

# Average approach

mean_stp = round(inner_joined_station_and_main['stp'].mean(), 2)
mean_slp = round(inner_joined_station_and_main['slp'].mean(), 2)


mean_tryout['stp'] = mean_tryout['stp'].fillna(mean_stp)
mean_tryout['slp'] = mean_tryout['slp'].fillna(mean_slp)



# Median approach
median_stp = round(inner_joined_station_and_main['stp'].median(), 2)
median_slp = round(inner_joined_station_and_main['slp'].median(), 2)


median_tryout['stp'] = median_tryout['stp'].fillna(median_stp)
median_tryout['slp'] = median_tryout['slp'].fillna(median_slp)


# Linear interpolation approach
linear_interpolation_tryout.replace(pd.NA, np.nan, inplace=True)

linear_interpolation_tryout = linear_interpolation_tryout.sort_values('date')
linear_interpolation_tryout = linear_interpolation_tryout.set_index('date')

linear_interpolation_tryout['stp'] = linear_interpolation_tryout['stp'].interpolate(method='time')
linear_interpolation_tryout['slp'] = linear_interpolation_tryout['slp'].interpolate(method='time')

linear_interpolation_tryout = linear_interpolation_tryout.reset_index()

print('--------------------------------')
print('ORIGINAL values for innr_joined_station_and_main:\n')
print(f'stp: \nmean: {round(inner_joined_station_and_main['stp'].mean(),2)}\nstd: {round(inner_joined_station_and_main['stp'].std(), 2)}')
print(f'slp: \nmean: {round(inner_joined_station_and_main['slp'].mean(),2)}\nstd: {round(inner_joined_station_and_main['slp'].std(), 2)}')
print('--------------------------------\n')
print('After MEAN value imputation:\n')
print(f'stp: \nmean: {round(mean_tryout['stp'].mean(), 2)}\nstd: {round(mean_tryout['stp'].std(), 2)}')
print(f'slp: \nmean: {round(mean_tryout['slp'].mean(), 2)}\nstd: {round(mean_tryout['slp'].std(), 2)}')
print('--------------------------------\n')
print('After MEDIAN value imputation:\n')
print(f'stp: \nmean: {round(median_tryout['stp'].mean(), 2)}\nstd: {round(median_tryout['stp'].std(), 2)}')
print(f'slp: \nmean: {round(median_tryout['slp'].mean(), 2)}\nstd: {round(median_tryout['slp'].std(), 2)}')
print('--------------------------------\n')
print('After INTERPOLATION value imputation:\n')
print(f'stp: \nmean: {round(linear_interpolation_tryout['stp'].mean(), 2)}\nstd: {round(linear_interpolation_tryout['stp'].std(), 2)}')
print(f'slp: \nmean: {round(linear_interpolation_tryout['slp'].mean(), 2)}\nstd: {round(linear_interpolation_tryout['slp'].std(), 2)}')

data_loader.save_df_to_csv(
    mean_tryout,
    'data/report/mean_tryout.csv'
)

data_loader.save_df_to_csv(
    median_tryout,
    'data/report/median_tryout.csv'
)

data_loader.save_df_to_csv(
    linear_interpolation_tryout,
    'data/report/linear_interpolation_tryout.csv'
)

# WNIOSEK: INTEROPLACJA TO NIEZLY SZEFUNCIU