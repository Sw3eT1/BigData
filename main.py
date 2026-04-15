import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.metrics import r2_score
from dotenv import load_dotenv

# Import naszych wlasnych klas z innych plikow
from data_loader_class import DataLoader
from data_frame_manipulator_class import DataFrameManipulator

# ==========================================================
# BOILERPLATE - LADOWANIE DANYCH
# ==========================================================
load_dotenv()
KEY_FILE = os.environ.get('KEYFILEPATH')
data_loader = DataLoader(KEY_FILE)
data_manipulator = DataFrameManipulator()


def load_data_with_data_loader_using_query():
    data_loader.make_a_query_and_save_to_class("SELECT * FROM `bigquery-public-data.noaa_gsod.gsod2020`",
                                               'basic_2020_query')
    data_loader.make_a_query_and_save_to_class("SELECT * FROM `bigquery-public-data.noaa_gsod.gsod2021`",
                                               'basic_2021_query')
    data_loader.make_a_query_and_save_to_class("SELECT * FROM bigquery-public-data.noaa_gsod.stations",
                                               'stations_query')
    data_loader.save_all_df_to_csv()


loaded = data_loader.load_dfs_from_csv_to_class()

if loaded == 0:
    print('Downloading data from the server (this will only take a moment the first time)...')
    load_data_with_data_loader_using_query()

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



data_manipulator.change_column_names({'usaf':'stn'}, station_main_data)


inner_joined_station_and_main = data_manipulator.join_two_dfs(all_main_data, station_main_data, ['stn', 'wban'],)

# Zostawiamy w bazie tylko Norwegię i Brazylię (potrzebne do Części 5)
test_countries = ['NO', 'BR']
country_comparison_data = inner_joined_station_and_main[
    inner_joined_station_and_main['country'].isin(test_countries)
].copy()

# ==========================================================
# PRZYGOTOWANIE DANYCH POD SZEREGI CZASOWE
# ==========================================================
print("\nPreparing global time aggregates...")

# Globalna srednia pogoda (kompresja do 2 lat)
daily_weather = inner_joined_station_and_main.groupby('date')[
    ['temp', 'prcp', 'wdsp', 'slp', 'visib']].mean().reset_index()
daily_weather = daily_weather.sort_values('date').set_index('date').interpolate(method='time').reset_index()
daily_weather['day_index'] = np.arange(len(daily_weather))

# Dane rolnicze z pliku CSV
crop_data = pd.read_csv('data/Production_Crops_Livestock_E_All_Data_NOFLAG.csv', low_memory=False)
crop_data = crop_data[(crop_data['Element'] == 'Production') & (crop_data['Item'] == 'Barley')]
year_cols = [col for col in crop_data.columns if col.startswith('Y') and col[1:].isdigit()]
crop_melted = crop_data.melt(id_vars=['Area'], value_vars=year_cols, var_name='year', value_name='production')
crop_melted['year'] = crop_melted['year'].str.replace('Y', '').astype(int)
crop_melted.rename(columns={'Area': 'country_name'}, inplace=True)

# Globalna produkcja
yearly_agri = crop_melted.groupby('year')['production'].sum().reset_index()
yearly_agri['production'] = yearly_agri['production'].interpolate()

# ==========================================================
# PART 1: Srednia kroczaca (Trendy)
# ==========================================================
print("\n--- Part 1: Calculating moving averages ---")

variables_weather = ['temp', 'prcp', 'wdsp', 'slp', 'visib']
for var in variables_weather:
    daily_weather[f'{var}_roll_mean'] = daily_weather[var].rolling(window=30, center=True).mean()
    daily_weather[f'{var}_roll_std'] = daily_weather[var].rolling(window=30, center=True).std()

yearly_agri['prod_roll_mean'] = yearly_agri['production'].rolling(window=5, center=True).mean()
yearly_agri['prod_roll_std'] = yearly_agri['production'].rolling(window=5, center=True).std()

fig, ax = plt.subplots(3, 2, figsize=(14, 5))
ax[0][0].plot(daily_weather['date'], daily_weather['temp'], alpha=0.3, label='Daily Temp', color='blue')
ax[0][0].plot(daily_weather['date'], daily_weather['temp_roll_mean'], linewidth=2, label='30-Day Trend (Mean)',
           color='red')
ax[0][0].plot(daily_weather['date'], daily_weather['temp_roll_std'], linewidth=2, label='30-Day Trend (Std)',
           color='yellow')
ax[0][0].set_title('Temperature with Moving Average')
ax[0][0].set_xlabel('Date')
ax[0][0].set_ylabel('Temperature [F]')
ax[0][0].legend()

ax[0][1].plot(daily_weather['date'], daily_weather['prcp'], alpha=0.3, label='Daily PRCP', color='blue')
ax[0][1].plot(daily_weather['date'], daily_weather['prcp_roll_mean'], linewidth=2, label='30-Day Trend (Mean)',
           color='red')
ax[0][1].plot(daily_weather['date'], daily_weather['prcp_roll_std'], linewidth=2, label='30-Day Trend (Std)',
           color='yellow')

ax[0][1].set_title('Precipitation with Moving Average')
ax[0][0].set_xlabel('Date')
ax[0][0].set_ylabel('Precipitation [XXXXX]')
ax[0][1].legend()

ax[1][0].plot(daily_weather['date'], daily_weather['wdsp'], alpha=0.3, label='Daily WDSP', color='blue')
ax[1][0].plot(daily_weather['date'], daily_weather['wdsp_roll_mean'], linewidth=2, label='30-Day Trend (Mean)',
           color='red')
ax[1][0].plot(daily_weather['date'], daily_weather['wdsp_roll_std'], linewidth=2, label='30-Day Trend (Std)',
           color='yellow')

ax[1][0].set_title('Wind speed with Moving Average')
ax[0][0].set_xlabel('Date')
ax[0][0].set_ylabel('Wind speed [knots]')
ax[1][0].legend()

ax[1][1].plot(daily_weather['date'], daily_weather['slp'], alpha=0.3, label='Daily slp', color='blue')
ax[1][1].plot(daily_weather['date'], daily_weather['slp_roll_mean'], linewidth=2, label='30-Day Trend (Mean)',
           color='red')
ax[1][1].plot(daily_weather['date'], daily_weather['slp_roll_std'], linewidth=2, label='30-Day Trend (Std)',
           color='yellow')

ax[1][1].set_title('SLP with Moving Average')
ax[0][0].set_xlabel('Date')
ax[0][0].set_ylabel('Pressure [Pa]')
ax[1][1].legend()


ax[2][0].plot(daily_weather['date'], daily_weather['visib'], alpha=0.3, label='Daily visib', color='blue')
ax[2][0].plot(daily_weather['date'], daily_weather['visib_roll_mean'], linewidth=2, label='30-Day Trend (Mean)',
           color='red')
ax[2][0].plot(daily_weather['date'], daily_weather['visib_roll_std'], linewidth=2, label='30-Day Trend (Std)',
           color='yellow')

ax[2][0].set_title('visib with Moving Average')
ax[0][0].set_xlabel('Date')
ax[0][0].set_ylabel('Visib [XXXX]')
ax[2][0].legend()

ax[2][1].plot(yearly_agri['year'], yearly_agri['production'], alpha=0.5, marker='o', label='Annual Production',
           color='green')
ax[2][1].plot(yearly_agri['year'], yearly_agri['prod_roll_mean'], linewidth=2, label='5-Year Trend Mean', color='orange',)
ax[2][1].plot(yearly_agri['year'], yearly_agri['prod_roll_std'], linewidth=2, label='5-Year Trend Std', color='yellow',)

ax[2][1].set_title('Annual production with Moving Average')
ax[0][0].set_xlabel('Year')
ax[0][0].set_ylabel('XXXX')
ax[2][1].legend()
plt.tight_layout()
plt.show()


# ==========================================================
# PART 2: Szeregi czasowe
# ==========================================================
print("\n--- Part 2: Time Series Analysis (Differencing Method) ---")


for var in variables_weather:
    daily_weather[f'{var}_diff'] = daily_weather[f'{var}'].diff()

fig, ax = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle('Temperature')
ax[0].plot(daily_weather['date'], daily_weather['temp_roll_mean'], color='red')
ax[0].set_title('Method 1: Smoothing (30d Moving Average) - Shows TREND and SEASONALITY')

ax[1].plot(daily_weather['date'], daily_weather['temp_diff'], color='purple', alpha=0.7)
ax[1].set_title('Method 2: Differencing (Day-to-day change) - Shows VARIABILITY (Noise/Peaks)')
ax[1].axhline(0, color='black', linestyle='--')
plt.tight_layout()
plt.show()

fig, ax = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle('Precipitation')
ax[0].plot(daily_weather['date'], daily_weather['prcp_roll_mean'], color='red')
ax[0].set_title('Method 1: Smoothing (30d Moving Average) - Shows TREND and SEASONALITY')

ax[1].plot(daily_weather['date'], daily_weather['prcp_diff'], color='purple', alpha=0.7)
ax[1].set_title('Method 2: Differencing (Day-to-day change) - Shows VARIABILITY (Noise/Peaks)')
ax[1].axhline(0, color='black', linestyle='--')
plt.tight_layout()
plt.show()

fig, ax = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle('Wind speed')
ax[0].plot(daily_weather['date'], daily_weather['wdsp_roll_mean'], color='red')
ax[0].set_title('Method 1: Smoothing (30d Moving Average) - Shows TREND and SEASONALITY')

ax[1].plot(daily_weather['date'], daily_weather['wdsp_diff'], color='purple', alpha=0.7)
ax[1].set_title('Method 2: Differencing (Day-to-day change) - Shows VARIABILITY (Noise/Peaks)')
ax[1].axhline(0, color='black', linestyle='--')
plt.tight_layout()
plt.show()

fig, ax = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle('Sea level pressure')
ax[0].plot(daily_weather['date'], daily_weather['slp_roll_mean'], color='red')
ax[0].set_title('Method 1: Smoothing (30d Moving Average) - Shows TREND and SEASONALITY')

ax[1].plot(daily_weather['date'], daily_weather['slp_diff'], color='purple', alpha=0.7)
ax[1].set_title('Method 2: Differencing (Day-to-day change) - Shows VARIABILITY (Noise/Peaks)')
ax[1].axhline(0, color='black', linestyle='--')
plt.tight_layout()
plt.show()

fig, ax = plt.subplots(1, 2, figsize=(14, 5))
fig.suptitle('Visibility')
ax[0].plot(daily_weather['date'], daily_weather['visib_roll_mean'], color='red')
ax[0].set_title('Method 1: Smoothing (30d Moving Average) - Shows TREND and SEASONALITY')

ax[1].plot(daily_weather['date'], daily_weather['visib_diff'], color='purple', alpha=0.7)
ax[1].set_title('Method 2: Differencing (Day-to-day change) - Shows VARIABILITY (Noise/Peaks)')
ax[1].axhline(0, color='black', linestyle='--')
plt.tight_layout()
plt.show()

# ==========================================================
# PART 3: Regresja Liniowa
# ==========================================================
print("\n--- Part 3: Training Linear Regression Models ---")


def plot_linear_regression(df, x_col, y_col, split_val, title, ax):
    train = df[df[x_col] < split_val]
    test = df[df[x_col] >= split_val]

    X_train, y_train = train[[x_col]].values, train[y_col].values
    X_test, y_test = test[[x_col]].values, test[y_col].values

    model = LinearRegression().fit(X_train, y_train)

    train_score = model.score(X_train, y_train)
    test_score = model.score(X_test, y_test)

    X_all = df[[x_col]].values
    y_all_pred = model.predict(X_all)

    ax.scatter(train[x_col], y_train, color='gray', alpha=0.5, label='Training')
    ax.scatter(test[x_col], y_test, color='blue', alpha=0.5, label='Test (Actual)')
    ax.plot(X_all, y_all_pred, color='red', linewidth=2,
            label=f'Regression (Train R2: {train_score:.2f}, Test R2: {test_score:.2f})')

    ax.set_title(title)
    ax.legend()


fig, axes = plt.subplots(3, 2, figsize=(14, 10))

split_day = 365
plot_linear_regression(daily_weather, 'day_index', 'temp', split_day, 'Linear Regression: Temperature', axes[0, 0])
plot_linear_regression(daily_weather, 'day_index', 'prcp', split_day, 'Linear Regression: Precipitation', axes[0, 1])
plot_linear_regression(daily_weather, 'day_index', 'wdsp', split_day, 'Linear Regression: Wind speed (WDSP)', axes[1, 0])
plot_linear_regression(daily_weather, 'day_index', 'slp', split_day, 'Linear Regression: Sea level pressure (SLP)', axes[1, 1])
plot_linear_regression(daily_weather, 'day_index', 'visib', split_day, 'Linear Regression: Visibility', axes[2, 0])
plot_linear_regression(yearly_agri, 'year', 'production', 2011, 'Linear Regression: Barley Production', axes[2, 1])

plt.tight_layout()
plt.show()

# ==========================================================
# PART 4: Regresja Wielomianowa
# ==========================================================
print("\n--- Part 4: Polynomial Regression Analysis ---")


def get_best_polynomial(df, x_col, y_col, split_val, max_degree=5):
    df_clean = df.dropna(subset=[x_col, y_col]).copy()

    train = df_clean[df_clean[x_col] < split_val]
    test = df_clean[df_clean[x_col] >= split_val]

    X_train, y_train = train[[x_col]].values, train[y_col].values
    X_test, y_test = test[[x_col]].values, test[y_col].values

    best_r2 = -float('inf')
    best_degree = 1
    best_model = None
    best_poly = None

    # szukanie najlepszego stopnia wielomianu
    for degree in range(1, max_degree + 1):
        poly = PolynomialFeatures(degree=degree)
        X_poly_train = poly.fit_transform(X_train)
        X_poly_test = poly.transform(X_test)

        model = LinearRegression().fit(X_poly_train, y_train)
        preds = model.predict(X_poly_test)
        r2 = r2_score(y_test, preds)

        if r2 > best_r2:
            best_r2 = r2
            best_degree = degree
            best_model = model
            best_poly = poly

    return best_degree, best_model, best_poly, best_r2


deg, model, poly, r2 = get_best_polynomial(yearly_agri, 'year', 'production', 2011)

X_all = yearly_agri[['year']].values
X_poly_all = poly.transform(X_all)
y_poly_pred = model.predict(X_poly_all)

plt.figure(figsize=(10, 5))
plt.scatter(yearly_agri['year'], yearly_agri['production'], color='green', alpha=0.6, label='Historical data')
plt.plot(X_all, y_poly_pred, color='purple', linewidth=3, label=f'Polynomial deg {deg} (R2: {r2:.2f})')
plt.title(f'Agricultural Production - Best Fit: Polynomial degree {deg}')
plt.legend()
plt.show()
print(f"The best model for agriculture is a polynomial of degree: {deg} with an R2 score: {r2:.2f}")

# ==========================================================
# PART 5: Porownanie Krajow (Norwegia vs Brazylia)
# ==========================================================
print("\n--- Part 5: Country Comparison (Norway North vs Brazil South) ---")

countries_weather = inner_joined_station_and_main[inner_joined_station_and_main['country'].isin(['NO', 'BR'])]
cw = countries_weather.groupby(['country', 'date'])[['temp']].mean().reset_index()
cw['date'] = pd.to_datetime(cw['date'])
cw = cw.dropna()

no_weather = cw[cw['country'] == 'NO'].copy()
br_weather = cw[cw['country'] == 'BR'].copy()
no_weather['day_idx'] = np.arange(len(no_weather))
br_weather['day_idx'] = np.arange(len(br_weather))

model_no = LinearRegression().fit(no_weather[['day_idx']].values, no_weather['temp'].values)
model_br = LinearRegression().fit(br_weather[['day_idx']].values, br_weather['temp'].values)

fig, ax = plt.subplots(figsize=(12, 6))

ax.plot(no_weather['date'], no_weather['temp'], color='blue', alpha=0.3, label='Norway (Raw)')
ax.plot(no_weather['date'], model_no.predict(no_weather[['day_idx']].values), color='darkblue', linewidth=3,
        label='Trend: Norway')

ax.plot(br_weather['date'], br_weather['temp'], color='orange', alpha=0.3, label='Brazil (Raw)')
ax.plot(br_weather['date'], model_br.predict(br_weather[['day_idx']].values), color='red', linewidth=3,
        label='Trend: Brazil')

plt.title('Climate and Trend Comparison: Northern (NO) vs Southern (BR) Hemisphere')
plt.ylabel('Temperature [°F]')
plt.legend()
plt.show()

print("Analysis completed successfully! All charts generated.")