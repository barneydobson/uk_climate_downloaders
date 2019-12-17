# -*- coding: utf-8 -*-
"""
This is a script to aggregate hadUK data, written by Barnaby Dobson, 5th, Dec, 2019
Data must have already been downloaded (see download_haduk_data.py)
"""

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import xarray as xr
import os
import haduk_downloader_functions
from tqdm import tqdm

"""Enter addresses and input data
"""
shp_address = os.path.join("C:\\","Users","bdobson","Downloads","example.shp")
data_address = os.path.join("C:\\","Users","bdobson","Downloads","")
output_address =  os.path.join("C:\\","Users","bdobson","Downloads","")

start_year = 1975
end_year = 1980
grid_scale = 25 # km
period = 'day' #'day', 'mon' or 'ann'
variable = 'rainfall' #tested 'rainfall' and 'tas' (temperature)

name = 'zone_name' # the 'name' column in the .shp file (or whatever column you want the output files to be named)

"""Load shapefile and create file names
"""
shp = gpd.read_file(shp_address).reset_index()

dates_series = haduk_downloader_functions.createDateseries(period,start_year,end_year)

file_name = haduk_downloader_functions.getFilename(variable,grid_scale,period)

"""Load data (needs netcdf4 installed)
"""
data = []
for period_text in tqdm(dates_series):
    ds = xr.open_dataset(data_address + file_name + period_text + '.nc')
    df = ds.to_dataframe()
    data.append(df)
data = pd.concat(data)

"""Remove grid points that have no data
"""
ind = data[variable] < 100000000
data = data.loc[ind].reset_index()

"""Assign grid points to the different shapes in the shapefile (this is slow)
"""
print('Assigning grid points')
grid_points = data[['latitude','longitude']].drop_duplicates()
points = []
for idx in tqdm(grid_points.index):
    row = grid_points.loc[idx]
    ind = np.where((data.latitude == row.latitude) & (data.longitude == row.longitude))
    points.append({'lon':row.longitude,'lat':row.latitude,'data_rows':ind,'index':str(idx),'geometry':Point(row.longitude,row.latitude)})
points = gpd.GeoDataFrame(points)
points = points.set_index('index')
points.crs = {'init' :'epsg:4326'}
points = gpd.tools.sjoin(points,shp,how='left')

"""Aggregate data and print to csv
"""
for i in shp['index']:
    point_ind = points[name] == shp.loc[i,name]
    points.loc[point_ind,'data_rows']
    in_geom = np.concatenate(np.concatenate(points.loc[point_ind,'data_rows']))
    
    data_in_geom = data.iloc[in_geom].reset_index()
    data_in_geom = data_in_geom.groupby('time').mean() # Average over all points inside a shape
    data_in_geom['date'] = data_in_geom.index.date.tolist()
    data_in_geom = data_in_geom[['date',variable]]
    
    iname = shp.loc[shp['index']==i,name].iloc[0]
    
    data_in_geom.to_csv(os.path.join(output_address,
                                     '_'.join([iname,
                                               variable,
                                               str(start_year),
                                               str(end_year),
                                               period,
                                               str(grid_scale),
                                               'km'])+".csv"),
                            sep=',',index=False)
