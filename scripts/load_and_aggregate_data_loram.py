# -*- coding: utf-8 -*-
"""
This is a script to aggregate hadUK data, written by Barnaby Dobson, 5th, Dec, 2019
Data must have already been downloaded (see download_haduk_data.py)

This script uses less RAM, loading only one dataset at a time. 
The aggregation is fast but may struggle if the shp file is too large
If this is the case, then use the data aggregation method from the script load_and_aggregate_data.py
"""

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import xarray as xr
import os
import haduk_downloader_functions
from tqdm import tqdm
import numpy as np

"""Enter addresses and input data
"""
shp_address = os.path.join("C:\\", "Users", "bdobson", "Documents", "GitHub", "wsimod", "projects", "fine_catchments", "data", "enfield", "raw", "INCA_subcatchments.shp")
data_address = os.path.join("C:\\", "Users", "bdobson", "Documents", "data", "haduk","")
output_address = data_address# os.path.join("C:\\", "Users", "bdobson", "Documents", "GitHub", "wsimod", "projects", "fine_catchments", "data", "enfield", "raw", "")

start_year = 2000
end_year = 2020 # Up to but not including the end_year
grid_scale = 1 # km
period = 'day' #'day', 'mon' or 'ann'
variable = 'rainfall' #tested 'rainfall' and 'tas' (temperature)

name = 'INCA_ID' # the 'name' column in the .shp file (or whatever column you want the output files to be named)

"""Load shapefile and create file names
"""
shp = gpd.read_file(shp_address).reset_index()

shp = shp.to_crs(epsg=4326)
dates_series = haduk_downloader_functions.createDateseries(period,start_year,end_year)

file_name = haduk_downloader_functions.getFilename(variable,grid_scale,period)

"""Load data (needs netcdf4 installed)
"""
initial = True
data = []
for period_text in tqdm(dates_series):
    ds = xr.open_dataset(data_address + file_name + period_text + '.nc')
    df = ds.to_dataframe()
    ind = df[variable] < 100000000
    df = df.loc[ind].reset_index()
    
    """Assign grid points to the different shapes in the shapefile
    If the points are the same in every file this only needs to be done once
    """
    if initial:
        print('Assigning grid points')
        grid_points = df[['latitude','longitude']].drop_duplicates().reset_index()
        points = []
        for idx in tqdm(grid_points.index):
            row = grid_points.loc[idx]
            points.append({'idx' : row['index'], 'lon':row.longitude, 'lat':row.latitude, 'geometry' : Point(row.longitude,row.latitude)})
            
        points = gpd.GeoDataFrame(points)
        points = points.set_index('idx')
        points.crs = '+init=epsg:4326'
        points = gpd.tools.sjoin(points,shp,how='left')
        points[['lat','lon']] = points[['lat','lon']].round(decimals = 6)
        initial = False
    
    
    df[['latitude','longitude']] = df[['latitude','longitude']].round(decimals = 6)
    df = pd.merge(df[['latitude','longitude','time',variable]], points[['lat','lon',name]], left_on=['latitude','longitude'], right_on = ['lat','lon'])
    df = df.dropna(subset = [name])
    df = df.groupby(['time','INCA_ID']).mean()
    df = df[variable].reset_index()
    df = df.pivot(index='time',columns = 'INCA_ID', values = variable)
    
    data.append(df)
data = pd.concat(data)
data.to_csv(os.path.join(output_address,
                                '_'.join([variable,
                                          str(start_year),
                                          str(end_year),
                                          period,
                                          str(grid_scale),
                                          'km','aggregated'])+".csv"),
                        sep=',',index=True)


