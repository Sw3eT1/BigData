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
    ['stn', 'wban', 'date']
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
    'stn', 'wban', 'name',
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

inner_joined_station_and_main = linear_interpolation_tryout.copy(True)

# # ----------------------------------------------------------
#
#
# ------------- Part 4 -------------------------------------
# ----------------------------------------------------------

analysis_df = linear_interpolation_tryout.copy()

analysis_df['date'] = pd.to_datetime(analysis_df['date'], errors='coerce')

# wybór miesiąca do analizy
selected_year = 2021
selected_month = 7

month_df = analysis_df[
    (analysis_df['date'].dt.year == selected_year) &
    (analysis_df['date'].dt.month == selected_month)
].copy()

# wybór 10 krajów z największą liczbą obserwacji
top_10_countries = (
    month_df['country']
    .dropna()
    .value_counts()
    .head(10)
    .index
    .tolist()
)

month_df = month_df[month_df['country'].isin(top_10_countries)].copy()

print("TOP 10 wybranych krajów:")
print(top_10_countries)

# ===========================
# 4.1 średnia temperatura, opady i prędkość wiatru
# ===========================
part_4_1 = (
    month_df
    .groupby('country')[['temp', 'prcp', 'wdsp']]
    .mean()
    .round(2)
    .reset_index()
)

print("\n4.1 Średnia temperatura, opady i prędkość wiatru:")
print(part_4_1)

data_loader.save_df_to_csv(
    part_4_1,
    'data/report/part_4_1_mean_by_country.csv'
)

# ===========================
# 4.2 średnia zmiana temperatury i opadów
# ===========================
month_df = month_df.sort_values(['country', 'date'])

month_df['temp_change'] = month_df.groupby('country')['temp'].diff()
month_df['prcp_change'] = month_df.groupby('country')['prcp'].diff()

part_4_2 = (
    month_df
    .groupby('country')[['temp_change', 'prcp_change']]
    .mean()
    .round(2)
    .reset_index()
)

print("\n4.2 Średnia zmiana temperatury i opadów:")
print(part_4_2)

data_loader.save_df_to_csv(
    part_4_2,
    'data/report/part_4_2_mean_change_by_country.csv'
)

# ===========================
# 4.3 mediana temperatury, opadów i prędkości wiatru
# ===========================
part_4_3 = (
    month_df
    .groupby('country')[['temp', 'prcp', 'wdsp']]
    .median()
    .round(2)
    .reset_index()
)

print("\n4.3 Mediana temperatury, opadów i prędkości wiatru:")
print(part_4_3)

data_loader.save_df_to_csv(
    part_4_3,
    'data/report/part_4_3_median_by_country.csv'
)

# ===========================
# 4.4 odchylenie standardowe temperatury, opadów i prędkości wiatru
# ===========================
part_4_4 = (
    month_df
    .groupby('country')[['temp', 'prcp', 'wdsp']]
    .std()
    .round(2)
    .reset_index()
)

print("\n4.4 Odchylenie standardowe temperatury, opadów i prędkości wiatru:")
print(part_4_4)

data_loader.save_df_to_csv(
    part_4_4,
    'data/report/part_4_4_std_by_country.csv'
)

# ===========================
# 4.5 minimalna, średnia i maksymalna temperatura, opadów i prędkości wiatru
# ===========================
part_4_5 = (
    month_df
    .groupby('country')[['temp', 'prcp', 'wdsp']]
    .agg(['min', 'mean', 'max'])
    .round(2)
)

part_4_5.columns = [f'{col}_{stat}' for col, stat in part_4_5.columns]
part_4_5 = part_4_5.reset_index()

print("\n4.5 Minimalna, średnia i maksymalna temperatura, opadów i prędkości wiatru:")
print(part_4_5)

data_loader.save_df_to_csv(
    part_4_5,
    'data/report/part_4_5_min_mean_max_by_country.csv'
)

# opcjonalnie moda, bo była wspomniana w treści
part_4_mode = (
    month_df
    .groupby('country')[['temp', 'prcp', 'wdsp']]
    .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else pd.NA)
    .reset_index()
)

print("\nDodatkowo moda temperatury, opadów i prędkości wiatru:")
print(part_4_mode)

data_loader.save_df_to_csv(
    part_4_mode,
    'data/report/part_4_mode_by_country.csv'
)

# ----------------------------------------------------------
# ------------- Part 5 -------------------------------------
# ----------------------------------------------------------

normalized_df = analysis_df.copy()

columns_to_normalize = [
    'temp',   # temperatura
    'prcp',   # opady
    'wdsp',   # prędkość wiatru
    'sndp',   # zmienna zastępcza "rolnicza"
    'visib',  # dodatkowa 1
    'slp',    # dodatkowa 2
    'stp'     # dodatkowa 3
]

for col in columns_to_normalize:
    col_min = normalized_df[col].min()
    col_max = normalized_df[col].max()

    if pd.notna(col_min) and pd.notna(col_max) and col_max != col_min:
        normalized_df[f'{col}_norm'] = (
            (normalized_df[col] - col_min) / (col_max - col_min)
        )
    else:
        normalized_df[f'{col}_norm'] = 0.0

data_loader.save_df_to_csv(
    normalized_df,
    'data/report/normalized_data.csv'
)

# ===========================
# 5.1 temperatura
# ===========================
fig, ax = plt.subplots(1, 2, figsize=(12, 5))

normalized_df.boxplot(column='temp', ax=ax[0])
ax[0].set_title('Temperatura przed normalizacją')
ax[0].set_ylabel('Temperature [F]')

normalized_df.boxplot(column='temp_norm', ax=ax[1])
ax[1].set_title('Temperatura po normalizacji')
ax[1].set_ylabel('Value [0, 1]')

plt.tight_layout()
plt.show()

# ===========================
# 5.2 opady
# ===========================
fig, ax = plt.subplots(1, 2, figsize=(12, 5))

normalized_df.boxplot(column='prcp', ax=ax[0])
ax[0].set_title('Opady przed normalizacją')
ax[0].set_ylabel('Precipitation [inch]')

normalized_df.boxplot(column='prcp_norm', ax=ax[1])
ax[1].set_title('Opady po normalizacji')
ax[1].set_ylabel('Value [0, 1]')

plt.tight_layout()
plt.show()

# ===========================
# 5.3 prędkość wiatru
# ===========================
fig, ax = plt.subplots(1, 2, figsize=(12, 5))

normalized_df.boxplot(column='wdsp', ax=ax[0])
ax[0].set_title('Prędkość wiatru przed normalizacją')
ax[0].set_ylabel('Wind speed [knot]')

normalized_df.boxplot(column='wdsp_norm', ax=ax[1])
ax[1].set_title('Prędkość wiatru po normalizacji')
ax[1].set_ylabel('Value [0, 1]')

plt.tight_layout()
plt.show()

# ===========================
# 5.4 zmienna zastępcza rolnicza: sndp
# ===========================
fig, ax = plt.subplots(1, 2, figsize=(12, 5))

normalized_df.boxplot(column='sndp', ax=ax[0])
ax[0].set_title('Pokrywa śnieżna przed normalizacją')
ax[0].set_ylabel('Snow depth')

normalized_df.boxplot(column='sndp_norm', ax=ax[1])
ax[1].set_title('Pokrywa śnieżna po normalizacji')
ax[1].set_ylabel('Value [0, 1]')

plt.tight_layout()
plt.show()

# ===========================
# 5.5 dodatkowe 3 zmienne
# ===========================
fig, ax = plt.subplots(1, 2, figsize=(14, 5))

normalized_df[['visib', 'slp', 'stp']].boxplot(ax=ax[0])
ax[0].set_title('Dodatkowe zmienne przed normalizacją')
ax[0].set_ylabel('Original scale')

normalized_df[['visib_norm', 'slp_norm', 'stp_norm']].boxplot(ax=ax[1])
ax[1].set_title('Dodatkowe zmienne po normalizacji')
ax[1].set_ylabel('Value [0, 1]')

plt.tight_layout()
plt.show()