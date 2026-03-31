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
    """, 'basic_2020_query')

    data_loader.make_a_query_and_save_to_class("""
        SELECT *
        FROM `bigquery-public-data.noaa_gsod.gsod2021`
        """, 'basic_2021_query')

    data_loader.make_a_query_and_save_to_class("""
    SELECT *
    FROM bigquery-public-data.noaa_gsod.stations
    """, 'stations_query')

    data_loader.save_all_df_to_csv()


data_manipulator = DataFrameManipulator()

loaded = data_loader.load_dfs_from_csv_to_class()
print(f'Liczba wczytanych plikow: {loaded}')

if loaded == 0:
    print('Pobieram dane z serwera (to moze chwile potrwac)...')
    load_data_with_data_loader_using_query()
    data_loader.save_all_df_to_csv()
    print('Dane pobrane i zapisane do folderu')


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

# ------------- Czesc 1: Wykresy pudelkowe -------------

# wywalamy sztuczne wartosci z bazy NOAA zeby nie psuly nam skali na wykresach
inner_joined_station_and_main.replace([99.99, 999.9, 9999.9], pd.NA, inplace=True)

# bierzemy probke 50k wierszy zeby nie wywalilo kompa przy rysowaniu tysiecy kropek
print("Losowanie probki 50tys wierszy do wykresow z Czesci 1...")
sample_for_plots = inner_joined_station_and_main.dropna(subset=['temp', 'max', 'min', 'prcp', 'visib', 'wdsp', 'slp', 'stp']).sample(n=50000, random_state=42)

# ===========================
# 1.1 - 1.3 Analiza temperatur
# ===========================
fig, ax = plt.subplots(1, 3, figsize=(18, 5))

sample_for_plots.boxplot(
    ax=ax[0],
    column='temp',
    by='year'
)
ax[0].set_title('Srednia temperatura na przestrzeni lat')
ax[0].set_ylabel('Temperatura [F]')

sample_for_plots.boxplot(
    ax=ax[1],
    column='max',
    by='year'
)
ax[1].set_title('Maksymalna temperatura')
ax[1].set_ylabel('Temperatura [F]')

sample_for_plots.boxplot(
    ax=ax[2],
    column='min',
    by='year'
)
ax[2].set_title('Minimalna temperatura')
ax[2].set_ylabel('Temperatura [F]')

plt.suptitle('Analiza temperatur')
plt.tight_layout()
plt.show()

# ===========================
# 1.4 i 1.6 Opady i widzialnosc
# ===========================
fig, ax = plt.subplots(1, 2, figsize=(12, 5))

sample_for_plots.boxplot(
    ax=ax[0],
    column='prcp',
    by='year'
)
ax[0].set_title('Opady atmosferyczne')
ax[0].set_ylabel('Opady [cale]')

sample_for_plots.boxplot(
    ax=ax[1],
    column='visib',
    by='year'
)
ax[1].set_title('Widzialnosc')
ax[1].set_ylabel('Widzialnosc [mile]')

plt.suptitle('Analiza opadow i widzialnosci')
plt.tight_layout()
plt.show()

# ===========================
# 1.5 Analiza wiatru
# ===========================
fig, ax = plt.subplots(1, 3, figsize=(18, 5))

sample_for_plots.boxplot(
    ax=ax[0],
    column='wdsp',
    by='year'
)
ax[0].set_title('Srednia predkosc wiatru')
ax[0].set_ylabel('Predkosc [wezly]')

sample_for_plots.boxplot(
    ax=ax[1],
    column='gust',
    by='year'
)
ax[1].set_title('Maksymalne porywy wiatru')
ax[1].set_ylabel('Predkosc [wezly]')

sample_for_plots.boxplot(
    ax=ax[2],
    column='mxpsd',
    by='year'
)
ax[2].set_title('Maksymalny ciagly wiatr')
ax[2].set_ylabel('Predkosc [wezly]')

plt.suptitle('Analiza wiatru')
plt.tight_layout()
plt.show()

# ===========================
# 1.7 Nasze dodatkowe zmienne (cisnienie)
# ===========================
fig, ax = plt.subplots(1, 2, figsize=(12, 5))

sample_for_plots.boxplot(
    ax=ax[0],
    column='slp',
    by='year'
)
ax[0].set_title('Cisnienie na poziomie morza (SLP)')
ax[0].set_ylabel('Cisnienie [mbar]')

sample_for_plots.boxplot(
    ax=ax[1],
    column='stp',
    by='year'
)
ax[1].set_title('Cisnienie na poziomie stacji (STP)')
ax[1].set_ylabel('Cisnienie [mbar]')

plt.suptitle('Analiza cisnienia atmosferycznego')
plt.tight_layout()
plt.show()


# ==========================================================
# Czesc 2: Misja dodatkowa (Identyfikacja outlierow IQR)
# ==========================================================
print("\n--- Czesc 2: Analiza Outlierow metoda IQR ---")

# bierzemy opady i wiatr do sprawdzenia
outlier_vars = ['prcp', 'wdsp']

for col in outlier_vars:
    # wymuszamy liczby, zeby funkcja liczenia kwartyli sie nie wywalila na stringach
    numeric_col = pd.to_numeric(inner_joined_station_and_main[col], errors='coerce').dropna()

    # liczymy kwartyle i rozstep (IQR)
    Q1 = numeric_col.quantile(0.25)
    Q3 = numeric_col.quantile(0.75)
    IQR = Q3 - Q1

    # wyznaczamy granice (te tzw. wasy na wykresie)
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    # lapiemy wszystko co wpadlo poza zakres
    outliers = numeric_col[(numeric_col < lower_bound) | (numeric_col > upper_bound)]

    print(f"Dla zmiennej '{col}': Znaleziono {len(outliers)} wartosci odstajacych (poza zakresem {lower_bound:.2f} do {upper_bound:.2f}).")


# ------------- Czesc 3: Obsluga brakow danych -------------

# ===========================
# 3.1 Zliczanie brakow
# ===========================
missing_values_report = data_manipulator.count_missing_values(
    inner_joined_station_and_main
)

data_loader.save_df_to_csv(
    missing_values_report,
    'data/report/missing_values_report.csv'
)

# ===========================
# 3.2 Podmienianie dziwnych placeholderow na prawidlowe pd.NA
# ===========================
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

# ===========================
# 3.3 Testowanie roznych metod łatania brakow dla cisnienia
# ===========================
mean_tryout = inner_joined_station_and_main.copy(True)
median_tryout = inner_joined_station_and_main.copy(True)
linear_interpolation_tryout = inner_joined_station_and_main.copy(True)

# 1 metoda: srednia
mean_stp = round(inner_joined_station_and_main['stp'].mean(), 2)
mean_slp = round(inner_joined_station_and_main['slp'].mean(), 2)

mean_tryout['stp'] = mean_tryout['stp'].fillna(mean_stp)
mean_tryout['slp'] = mean_tryout['slp'].fillna(mean_slp)

# 2 metoda: mediana
median_stp = round(inner_joined_station_and_main['stp'].median(), 2)
median_slp = round(inner_joined_station_and_main['slp'].median(), 2)

median_tryout['stp'] = median_tryout['stp'].fillna(median_stp)
median_tryout['slp'] = median_tryout['slp'].fillna(median_slp)

# 3 metoda: interpolacja liniowa (czasowa)
linear_interpolation_tryout.replace(pd.NA, np.nan, inplace=True)

linear_interpolation_tryout = linear_interpolation_tryout.sort_values('date')
linear_interpolation_tryout = linear_interpolation_tryout.set_index('date')

linear_interpolation_tryout['stp'] = linear_interpolation_tryout['stp'].interpolate(method='time')
linear_interpolation_tryout['slp'] = linear_interpolation_tryout['slp'].interpolate(method='time')

linear_interpolation_tryout = linear_interpolation_tryout.reset_index()

# printy zeby porownac ktora metoda najmniej rozwalila nam rozklad
print('--------------------------------')
print('Wartosci oryginalne (przed lataniem):\n')
print(f"stp: \nsrednia: {round(inner_joined_station_and_main['stp'].mean(),2)}\nodchylenie: {round(inner_joined_station_and_main['stp'].std(), 2)}")
print(f"slp: \nsrednia: {round(inner_joined_station_and_main['slp'].mean(),2)}\nodchylenie: {round(inner_joined_station_and_main['slp'].std(), 2)}")
print('--------------------------------\n')
print('Po uzupelnieniu srednia:\n')
print(f"stp: \nsrednia: {round(mean_tryout['stp'].mean(), 2)}\nodchylenie: {round(mean_tryout['stp'].std(), 2)}")
print(f"slp: \nsrednia: {round(mean_tryout['slp'].mean(), 2)}\nodchylenie: {round(mean_tryout['slp'].std(), 2)}")
print('--------------------------------\n')
print('Po uzupelnieniu mediana:\n')
print(f"stp: \nsrednia: {round(median_tryout['stp'].mean(), 2)}\nodchylenie: {round(median_tryout['stp'].std(), 2)}")
print(f"slp: \nsrednia: {round(median_tryout['slp'].mean(), 2)}\nodchylenie: {round(median_tryout['slp'].std(), 2)}")
print('--------------------------------\n')
print('Po interpolacji liniowej:\n')
print(f"stp: \nsrednia: {round(linear_interpolation_tryout['stp'].mean(), 2)}\nodchylenie: {round(linear_interpolation_tryout['stp'].std(), 2)}")
print(f"slp: \nsrednia: {round(linear_interpolation_tryout['slp'].mean(), 2)}\nodchylenie: {round(linear_interpolation_tryout['slp'].std(), 2)}")

#data_loader.save_df_to_csv(
#    mean_tryout,
#    'data/report/mean_tryout.csv'
#)

#data_loader.save_df_to_csv(
#    median_tryout,
#    'data/report/median_tryout.csv'
#)

data_loader.save_df_to_csv(
    linear_interpolation_tryout,
    'data/report/linear_interpolation_tryout.csv'
)

#  Interpolacja wypada najsensowniej, wiec nadpisujemy glowna tabele jej wynikiem
inner_joined_station_and_main = linear_interpolation_tryout.copy(True)


# ------------- Czesc 4: Obliczenia statystyczne -------------

analysis_df = linear_interpolation_tryout.copy()
analysis_df['date'] = pd.to_datetime(analysis_df['date'], format='%Y-%m-%d', errors='coerce')

# wybieramy lipiec 2021 jako nasz miesiac testowy
selected_year = 2021
selected_month = 7

month_df = analysis_df[
    (analysis_df['date'].dt.year == selected_year) &
    (analysis_df['date'].dt.month == selected_month)
].copy()

# lapiemy top 10 państw zeby miec na czym dzialac
top_10_countries = (
    month_df['country']
    .dropna()
    .value_counts()
    .head(10)
    .index
    .tolist()
)

month_df = month_df[month_df['country'].isin(top_10_countries)].copy()

print("\nTOP 10 wylosowanych krajow do analizy:")
print(top_10_countries)

# ===========================
# 4.1 Srednie wartosci
# ===========================
part_4_1 = (
    month_df
    .groupby('country')[['temp', 'prcp', 'wdsp']]
    .mean()
    .round(2)
    .reset_index()
)

print("\n4.1 Srednia temperatura, opady i wiatr:")
print(part_4_1)

data_loader.save_df_to_csv(
    part_4_1,
    'data/report/part_4_1_mean_by_country.csv'
)

# ===========================
# 4.2 Zmiany dzienne (diff)
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

print("\n4.2 Srednia zmiana temperatury i opadow z dnia na dzien:")
print(part_4_2)

data_loader.save_df_to_csv(
    part_4_2,
    'data/report/part_4_2_mean_change_by_country.csv'
)

# ===========================
# 4.3 Mediany
# ===========================
part_4_3 = (
    month_df
    .groupby('country')[['temp', 'prcp', 'wdsp']]
    .median()
    .round(2)
    .reset_index()
)

print("\n4.3 Mediana temperatury, opadow i wiatru:")
print(part_4_3)

data_loader.save_df_to_csv(
    part_4_3,
    'data/report/part_4_3_median_by_country.csv'
)

# ===========================
# 4.4 Odchylenie standardowe
# ===========================
part_4_4 = (
    month_df
    .groupby('country')[['temp', 'prcp', 'wdsp']]
    .std()
    .round(2)
    .reset_index()
)

print("\n4.4 Odchylenie standardowe:")
print(part_4_4)

data_loader.save_df_to_csv(
    part_4_4,
    'data/report/part_4_4_std_by_country.csv'
)

# ===========================
# 4.5 Min, Srednia i Max razem
# ===========================
part_4_5 = (
    month_df
    .groupby('country')[['temp', 'prcp', 'wdsp']]
    .agg(['min', 'mean', 'max'])
    .round(2)
)

# splaszczamy nazwy kolumn zeby ladnie wygladalo w csv
part_4_5.columns = [f'{col}_{stat}' for col, stat in part_4_5.columns]
part_4_5 = part_4_5.reset_index()

print("\n4.5 Wartosci MIN, AVG i MAX dla wybranych parametrow:")
print(part_4_5)

data_loader.save_df_to_csv(
    part_4_5,
    'data/report/part_4_5_min_mean_max_by_country.csv'
)

# opcjonalnie doliczamy mode
part_4_mode = (
    month_df
    .groupby('country')[['temp', 'prcp', 'wdsp']]
    .agg(lambda x: x.mode().iloc[0] if not x.mode().empty else pd.NA)
    .reset_index()
)

print("\nModa (najczestsza wartosc):")
print(part_4_mode)

data_loader.save_df_to_csv(
    part_4_mode,
    'data/report/part_4_mode_by_country.csv'
)

# ==========================================================
# 4.6 Zestawienie pogody z plonami rolniczymi
# ==========================================================
print("\n4.6 Analiza relacji: Pogoda vs Rolnictwo")

try:
    # wczytujemy lokalny plik z baza rolnicza z poprzedniego zadania
    crop_data = pd.read_csv('data/Production_Crops_Livestock_E_All_Data_NOFLAG.csv', low_memory=False)

    # zostawiamy tylko rubryke z ostateczna produkcja na badane lata
    crop_data = crop_data[crop_data['Element'] == 'Production']
    crop_data = crop_data[['Area', 'Item', 'Y2020', 'Y2021']]

    # odpivotowanie (melt) - zamieniamy lata z kolumn na normalne wiersze
    crop_melted = crop_data.melt(id_vars=['Area', 'Item'], value_vars=['Y2020', 'Y2021'], var_name='year',
                                 value_name='production_value')
    # wywalamy literke Y zeby rok stal sie normalna liczba
    crop_melted['year'] = crop_melted['year'].str.replace('Y', '').astype(int)
    crop_melted.rename(columns={'Area': 'country_name', 'Item': 'crop_type'}, inplace=True)

    filtered_weather = inner_joined_station_and_main[
        inner_joined_station_and_main['year'].isin([2020, 2021])
    ]

    # wyliczamy srednia temp. caloroczna z bazy pogody, zeby miec z czym to połączyć
    weather_yearly = filtered_weather.groupby(['country', 'year']).agg(
        avg_yearly_temp=('temp', 'mean')
    ).reset_index()

    # slownik tlumaczacy glupie kody krajow z NOAA na prawidlowe nazwy panstw z FAOSTAT
    country_mapping = {
        'NO': 'Norway', 'ES': 'El Salvador', 'AF': 'Afghanistan',
        'US': 'United States of America', 'CA': 'Canada', 'RS': 'Russian Federation',
        'AS': 'Australia', 'CH': 'China', 'BR': 'Brazil', 'JA': 'Japan',
        'UK': 'United Kingdom of Great Britain and Northern Ireland',
        'SW': 'Sweden', 'FR': 'France', 'PL': 'Poland', 'GM': 'Germany'
    }
    weather_yearly['country_name'] = weather_yearly['country'].map(country_mapping)
    weather_yearly.dropna(subset=['country_name'], inplace=True)

    # inner join - zlaczenie dwoch swiatow w calosc
    crop_weather_df = pd.merge(weather_yearly, crop_melted, on=['country_name', 'year'], how='inner')

    # bierzemy pod lupe Jeczmien
    selected_crop = 'Barley'
    selected_year_crop = 2021

    crop_subset = crop_weather_df[
        (crop_weather_df['year'] == selected_year_crop) &
        (crop_weather_df['crop_type'] == selected_crop)
        ].copy()

    if not crop_subset.empty:
        # lapiemy top 10 najwiekszych producentow tej rosliny
        top_10_crop_countries = crop_subset.nlargest(10, 'production_value')

        # rysowanie podwojnego wykresu (zeby dalo sie porownac stopnie z tonami na jednym ekranie)
        fig, ax1 = plt.subplots(figsize=(12, 6))
        x_indices = np.arange(len(top_10_crop_countries['country_name']))

        # lewa os: slupki z plonami
        ax1.bar(x_indices, top_10_crop_countries['production_value'], width=0.4, color='forestgreen',
                label=f'Produkcja {selected_crop}')
        ax1.set_ylabel('Produkcja (tony)', color='forestgreen', fontsize=12)
        ax1.set_xticks(x_indices)
        ax1.set_xticklabels(top_10_crop_countries['country_name'], rotation=45, ha='right')

        # prawa os: linia z temperatura
        ax2 = ax1.twinx()
        ax2.plot(x_indices, top_10_crop_countries['avg_yearly_temp'], color='crimson', marker='o',
                 linestyle='none', linewidth=2, markersize=8, label='Sr. Temp [F]')
        ax2.set_ylabel('Srednia Temperatura [F]', color='crimson', fontsize=12)

        plt.title(f'Produkcja: Jeczmien a srednia temperatura w {selected_year_crop} roku', fontsize=14,
                  fontweight='bold')
        fig.tight_layout()
        plt.show()

        data_loader.save_df_to_csv(top_10_crop_countries, 'data/report/part_4_6_crop_vs_weather.csv')

    else:
        print(f"Ups, brak danych dla uprawy {selected_crop}.")

except Exception as e:
    print(f"Problem przy tworzeniu punktu 4.6: {e}")


# ------------- Czesc 5: Normalizacja Danych -------------

normalized_df = analysis_df.copy()

# decydujemy ktore rubryki chcemy splaszczyc do skali 0-1
columns_to_normalize = [
    'temp',
    'prcp',
    'wdsp',
    'sndp',
    'visib',
    'slp',
    'stp'
]

# petla lecąca standardowym algorytmem Min-Max
for col in columns_to_normalize:
    col_min = normalized_df[col].min()
    col_max = normalized_df[col].max()

    if pd.notna(col_min) and pd.notna(col_max) and col_max != col_min:
        normalized_df[f'{col}_norm'] = (
            (normalized_df[col] - col_min) / (col_max - col_min)
        )
    else:
        # jak wszedzie jest to samo to dajemy po prostu zero
        normalized_df[f'{col}_norm'] = 0.0

data_loader.save_df_to_csv(
    normalized_df,
    'data/report/normalized_data.csv'
)

print("Losowanie probki 50 tys wierszy zeby narysowac normalizacje bez zacinania")
normalized_sample = normalized_df.dropna(subset=['temp', 'prcp', 'wdsp', 'visib', 'slp', 'stp']).sample(n=50000, random_state=42)

# ===========================
# 5.1 Temperatura przed/po
# ===========================
fig, ax = plt.subplots(1, 2, figsize=(12, 5))

normalized_sample.boxplot(column='temp', ax=ax[0])
ax[0].set_title('Temperatura przed normalizacja')
ax[0].set_ylabel('Temperatura [F]')

normalized_sample.boxplot(column='temp_norm', ax=ax[1])
ax[1].set_title('Temperatura po normalizacji')
ax[1].set_ylabel('Wartosc znormalizowana [0 - 1]')

plt.tight_layout()
plt.show()

# ===========================
# 5.2 Opady przed/po
# ===========================
fig, ax = plt.subplots(1, 2, figsize=(12, 5))

normalized_sample.boxplot(column='prcp', ax=ax[0])
ax[0].set_title('Opady przed normalizacja')
ax[0].set_ylabel('Opady [cale]')

normalized_sample.boxplot(column='prcp_norm', ax=ax[1])
ax[1].set_title('Opady po normalizacji')
ax[1].set_ylabel('Wartosc znormalizowana [0 - 1]')

plt.tight_layout()
plt.show()

# ===========================
# 5.3 Wiatr przed/po
# ===========================
fig, ax = plt.subplots(1, 2, figsize=(12, 5))

normalized_sample.boxplot(column='wdsp', ax=ax[0])
ax[0].set_title('Wiatr przed normalizacja')
ax[0].set_ylabel('Wiatr [wezly]')

normalized_sample.boxplot(column='wdsp_norm', ax=ax[1])
ax[1].set_title('Wiatr po normalizacji')
ax[1].set_ylabel('Wartosc znormalizowana [0 - 1]')

plt.tight_layout()
plt.show()

# ===========================
# 5.4 Zmienna rolnicza (plony z pkt 4.6) przed/po
# ===========================
if 'crop_subset' in locals() and not crop_subset.empty:
    prod_min = crop_subset['production_value'].min()
    prod_max = crop_subset['production_value'].max()

    crop_subset['production_value_norm'] = (crop_subset['production_value'] - prod_min) / (prod_max - prod_min)

    fig, ax = plt.subplots(1, 2, figsize=(12, 5))

    crop_subset.boxplot(column='production_value', ax=ax[0])
    ax[0].set_title(f'Plony ({selected_crop}) przed normalizacja')
    ax[0].set_ylabel('Produkcja [tony]')

    crop_subset.boxplot(column='production_value_norm', ax=ax[1])
    ax[1].set_title('Plony po normalizacji')
    ax[1].set_ylabel('Wartosc znormalizowana [0 - 1]')

    plt.tight_layout()
    plt.show()

# ===========================
# 5.5 Zmienne dodatkowe (skala przed i po scisnieciu w [0-1])
# ===========================
fig, ax = plt.subplots(1, 2, figsize=(14, 5))

normalized_sample[['visib', 'slp', 'stp']].boxplot(ax=ax[0])
ax[0].set_title('Dodatkowe zmienne na poczatku (rozstrzelona skala)')
ax[0].set_ylabel('Oryginalna jednostka')

normalized_sample[['visib_norm', 'slp_norm', 'stp_norm']].boxplot(ax=ax[1])
ax[1].set_title('Te same zmienne po normalizacji')
ax[1].set_ylabel('Wartosc znormalizowana [0 - 1]')

plt.tight_layout()
plt.show()