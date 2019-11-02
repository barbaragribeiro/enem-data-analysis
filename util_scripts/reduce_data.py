import csv
import pandas as pd
import dask.dataframe as dd

path2018 = 'microdados_enem2018/DADOS/'
path2017 = 'microdados_enem2017/Microdados Enem 2017/DADOS/'
path2016 = 'microdados_enem2016/Microdados_enem_2016/DADOS/'
file = 'MICRODADOS_ENEM_'

nth_row = 1000

df2018 = dd.read_csv(path2018+file+'2018.csv', sep=';',  encoding='latin-1', dtype='object')
df2018['df_index'] = df2018.index
df_2018_smaller = df2018[df2018['df_index'] % nth_row == 0]
final_2018_df = df_2018_smaller.compute()
export_csv = final_2018_df.to_csv (r'Microdados_ENEM_Amostra_2018.csv')
print("Dataframe 2018 processado.")


df2017 = dd.read_csv(path2017+file+'2017.csv', sep=';', encoding='latin-1', dtype='object')
df2017['df_index'] = df2017.index
df_2017_smaller = df2017[df2017['df_index'] % nth_row == 0]
final_2017_df = df_2017_smaller.compute()
export_csv = final_2017_df.to_csv (r'Microdados_ENEM_Amostra_2017.csv')
print("Dataframe 2017 processado.")



df2016 = dd.read_csv(path2016+'microdados_enem_2016.csv', sep=';', encoding='latin-1', dtype='object')
df2016['df_index'] = df2016.index
df_2016_smaller = df2016[df2016['df_index'] % nth_row == 0]
final_2016_df = df_2016_smaller.compute()
export_csv = final_2016_df.to_csv (r'Microdados_ENEM_Amostra_2016.csv')
print("Dataframe 2016 processado.")