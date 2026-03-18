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
    'NO', 'ES', 'AF'
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


print("Merging weather data with crops...")

# 6.1 Ładujemy czysty plik bez flag

crop_data = data_loader.get_df_from_csv('data/Production_Crops_Livestock_E_All_Data_NOFLAG.csv')

# 6.2 Bierzemy tylko to co nam potrzebne
columns_needed_crop = ['Area', 'Item', 'Element', 'Unit', 'Y2020', 'Y2021']
crop_data = data_manipulator.use_only_columns_needed(crop_data, columns_needed_crop)

# Wywalamy inne metryki, zostawiamy samą produkcję
crop_data = crop_data[crop_data['Element'] == 'Production']

# 6.3 Przerabiamy dane z szerokich na długie
crop_data_melted = crop_data.melt(
    id_vars=['Area', 'Item', 'Element', 'Unit'],
    value_vars=['Y2020', 'Y2021'],
    var_name='Year',
    value_name='Value'
)

# 6.4 Ogarniamy kolumnę Year żeby pasowała do tej z pogody
crop_data_melted['Year'] = crop_data_melted['Year'].str.replace('Y', '')
crop_data_melted['Year'] = pd.to_numeric(crop_data_melted['Year'], errors='coerce')

# Podmieniamy nazwy kolumn, żeby ładnie się złączyły z naszymi z NOAA
data_manipulator.change_column_names(
    {
        'Area': 'country_name',
        'Item': 'crop_type',
        'Year': 'year',
        'Value': 'production_value'
    },
    crop_data_melted
)

# 6.5 Zwijamy dane pogodowe z dniówek na roczne, bierzemy zbiór z 5 punktu
weather_yearly = inner_joined_station_and_main.copy()
weather_yearly['year'] = pd.to_numeric(weather_yearly['year'], errors='coerce')

weather_yearly_agg = weather_yearly.groupby(['country', 'year']).agg(
    avg_yearly_temp=('temp', 'mean'),
    total_yearly_prcp=('prcp', 'sum'),
    avg_yearly_wdsp=('wdsp', 'mean')
).reset_index()

# 6.6 Tłumaczenie skrótów krajów z pogody na pełne nazwy z bazy plonów
country_mapping = {
    'NO': 'Norway',
    'ES': 'El Salvador',
    'AF': 'Afghanistan'
}

weather_yearly_agg['country_name'] = weather_yearly_agg['country'].map(country_mapping)

# Wywalamy te wpisy z pogody, dla których nie daliśmy tłumaczenia w słowniku wyżej
weather_yearly_agg.dropna(subset=['country_name'], inplace=True)

# 7. Zlepiamy oba zbiory
final_combined_data = data_manipulator.join_two_dfs(
    weather_yearly_agg,
    crop_data_melted,
    join_columns=['country_name', 'year'],
    type_of_join='inner'
)

# 8. Zapisujemy końcowy raport i fajrant
if final_combined_data is not None:
    data_loader.save_df_to_csv(
        final_combined_data,
        'data/report/weather_and_crop_production_final.csv'
    )
    print("Stage 6 completed. Results can be found in 'data/report/weather_and_crop_production_final.csv'. ")
else:
    print("Error with merging data")