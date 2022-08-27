import csv
import pandas as pd
import geopandas as gpd
import io
from pathlib import Path
from functools import reduce

#******** Crop Data*********************************************
df_crop = pd.read_csv('/Users/puja_rulz/Downloads/PatternAgTechnical/Inputs/crop.csv')
cp_union = gpd.GeoDataFrame(
    df_crop.loc[:, [c for c in df_crop.columns if c != "field_geometry"]],
    geometry=gpd.GeoSeries.from_wkt(df_crop["field_geometry"]),
    crs="epsg:4326",)
cp=cp_union[cp_union['year']==2021]
cp = cp.rename({'geometry': 'field_geometry'}, axis='columns')
header = ['field_id', 'field_geometry','crop_type']
crop_filtered_data=cp.filter(header)

crop_filtered_data.to_csv('/Users/puja_rulz/Downloads/PatternAgTechnical/Outputs/Crop_output.csv')

#***********Spectral Data*******************************************

df_spectral = pd.read_csv('/Users/puja_rulz/Downloads/PatternAgTechnical/Inputs/spectral.csv')
cp_union = gpd.GeoDataFrame(
    df_spectral.loc[:, [c for c in df_spectral.columns if c != "tile_geometry"]],
    geometry=gpd.GeoSeries.from_wkt(df_spectral["tile_geometry"]),
    crs="epsg:4326",)
cp_union["Pos"]=(cp_union['nir']-cp_union['red'])/(cp_union['nir']+cp_union['red'])
cp_union["year"]=pd.to_datetime(cp_union['date']).dt.year
cp=cp_union[cp_union["year"]==2021]
cp = cp.rename({'geometry': 'tile_geometry','date':'Pos_date'}, axis='columns')
cp=cp[cp["Pos"]==cp["Pos"].max()]
header = ['tile_id', 'tile_geometry', 'Pos', 'Pos_date']
spectral_filtered_data=cp.filter(header)
spectral_filtered_data.to_csv('/Users/puja_rulz/Downloads/PatternAgTechnical/Outputs/spectral_output.csv')

#***********Soil Data*******************************************

df_soil = pd.read_csv('/Users/puja_rulz/Downloads/PatternAgTechnical/Inputs/soil.csv')
cp_union = gpd.GeoDataFrame(
    df_soil.loc[:, [c for c in df_soil.columns if c != "mukey_geometry"]],
    geometry=gpd.GeoSeries.from_wkt(df_soil["mukey_geometry"]),
    crs="epsg:4326",)
cp_union['Weighted_Hl']=abs(cp_union['hzdept']-cp_union['hzdepb'])/cp_union['hzdepb']
header = ['mukey', 'mukey_geometry', 'om', 'cec', 'ph']
soil_filtered_data=cp_union.filter(header)
soil_filtered_data.to_csv('/Users/puja_rulz/Downloads/PatternAgTechnical/Outputs/soil_output.csv')

#***********Weather Data*******************************************

df_weather = pd.read_csv('/Users/puja_rulz/Downloads/PatternAgTechnical/Inputs/weather.csv')
df_weather = pd.DataFrame(df_weather)
df_weather=df_weather[df_weather["year"]==2021]
print(df_weather)
df_precip = df_weather.groupby('fips_code')['precip'].sum()
df_maxtemp = df_weather.groupby('fips_code')['temp'].max()
df_mintemp = df_weather.groupby('fips_code')['temp'].min()
df_meantemp= df_weather.groupby('fips_code')['temp'].mean()
data_merge = reduce(lambda left, right:     # Merge three pandas DataFrames
                     pd.merge(left , right,
                              on = ["fips_code"]),
                     [df_precip, df_maxtemp, df_mintemp,df_meantemp])
data_merge = data_merge.rename({'temp': 'mean_temp','temp_y':'min_temp','temp_x':'max_temp'}, axis='columns')
data_merge.to_csv('/Users/puja_rulz/Downloads/PatternAgTechnical/Outputs/weather_output.csv')

    
