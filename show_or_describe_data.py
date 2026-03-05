import pandas as pd

from load_data import df_stations

# 4.1. Chcemy posiadać podstawowe informacje o lokalizacjach pomiarów pogodowych (stacje) oraz krajach,
# tak aby dane były zrozumiałe dla człowieka i możliwe do dalszego przetwarzania
df_stations_per_country = pd.read_csv('merged_gsod.csv', usecols=['stn', 'wban', 'name','country','state'])

print(df_stations_per_country)
