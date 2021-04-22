# -*- coding: utf-8 -*-
"""
Created on Thu Apr 22 09:11:01 2021

@author: bdobson
"""

import os
import haduk_downloader_functions
import geopandas as gpd
from shapely.geometry import Point
import xarray as xr
import tarfile
import pandas as pd
from tqdm import tqdm

#Downloads and formats on the fly due to file size

data={'username':'YOUR_USENAME',
      'password':'YOUR_PASSWORD'} # create an account here: https://services.ceda.ac.uk/cedasite/register/info/

output_folder = os.path.join("C:\\", "Users", "bdobson","Desktop","EA_request")

shp_fid = os.path.join("C:\\", "Users", "bdobson", "OneDrive - Imperial College London", "maps", "cams", "data", "Catchment_Abstraction_Management_Strategy_CAMS_Reference_Boundaries.shp")


site_url = 'https://auth.ceda.ac.uk/account/signin/' 

#Get shape
gdf = gpd.read_file(shp_fid)
gdf = gdf.loc[gdf.cams_name.isin(["East Hampshire","Arun and Western Streams","Test and Itchen"])]
gdf = gdf.to_crs("EPSG:4326")
name = "cams_name"

#Organise text
period = "baseline"
variable = "pepm" 
timestep = "daily"

file_url_base = 'http://data.ceda.ac.uk/badc/weather_at_home/data/marius_time_series/' + period + '/data' 

if period == 'baseline':
    shorthand = 'bs'
    years = range(1900,2006)
elif period == 'near_future':
    shorthand = 'nf'
    years = range(2020,2050)
elif period == 'far_future':
    shorthand = 'ff'
    years = range(2070,2100)

#Get stitching
stitchtable = pd.read_csv('stitching_table_' + shorthand + '.dat', sep=" ").set_index('year')

"""Login
"""
s = haduk_downloader_functions.startClient(data,site_url)



"""Iterate over years
"""
master_df = []

for i in tqdm(years):
  
    year = str(i)
    #Download file
    file_url = file_url_base + '/' + shorthand + '-' + year + '.tgz'
    req = s.get(file_url)
    
    fn = os.path.join(output_folder,shorthand + '-' + year + '.tgz')
    with open(fn,'wb') as f:
        f.write(req.content)
    
    #Extract tar
    dfs = []
    tar = tarfile.open(fn, "r:gz")

    for member in tar.getmembers():
        
        if (timestep in member.name) & (variable in member.name):
            stitch_code = member.name.split('/')[1]
            f = tar.extractfile(member)
            if f is not None:
                content = f.read()
                ds = xr.open_dataset(content)
                df = ds.to_dataframe()
                df = df.reset_index()[['lon','lat','time',variable]]
                df['stitch_code'] = stitch_code
                dfs.append(df)
                
    tar.close()
    dfs = pd.concat(dfs)
    dfs = dfs.dropna()
    
    #Get points and join
    grid_points = dfs[['lon','lat']].drop_duplicates()
    
    points = []
    for idx, row in grid_points.iterrows():
        points.append({'idx' : idx, 'lon':row.lon, 'lat':row.lat, 'geometry' : Point(row.lon,row.lat)})
    points = gpd.GeoDataFrame(points)
    points = points.set_index('idx')
    points.crs = 'epsg:4326'
    points = gpd.tools.sjoin(points,gdf,how='left')
    points = points.dropna()
    
    points = points[['lat','lon','cams_name']]
    
    #Create data
    merged = pd.merge(points, dfs, how='left', on = ['lat','lon'])
    
    #Stitch
    stitch_key = {v: k for k, v in stitchtable.loc[i].to_dict().items()}
    merged['ensemble'] = [stitch_key[x] for x in merged.stitch_code]
    merged = merged.drop('stitch_code', axis=1)
    master_df.append(merged)
    
    #Remove file
    os.remove(fn)

#Format and print    
master_df = pd.concat(master_df)
master_df = master_df.drop_duplicates()
master_df.to_csv(os.path.join(output_folder, period + '.csv' ))
mdf = master_df.copy()
mdf['time'] = master_df.time.astype(str)
mdf.to_parquet(os.path.join(output_folder, period + '.gzip' ), compression = 'GZIP')


#Custom format for request
shp_fid = os.path.join("C:\\", "Users", "bdobson", "OneDrive - Imperial College London","temp","jonny_shape.geojson")
jgdf = gpd.read_file(shp_fid)

grid_points = master_df[['lon','lat']].drop_duplicates()
    
points = []
for idx, row in grid_points.iterrows():
    points.append({'idx' : idx, 'lon':row.lon, 'lat':row.lat, 'geometry' : Point(row.lon,row.lat)})
points = gpd.GeoDataFrame(points)
points = points.set_index('idx')
points.crs = 'epsg:4326'
points = gpd.tools.sjoin(points,jgdf,how='left')
points = points.dropna()

merged = pd.merge(points, master_df, how='left', on = ['lat','lon'])
merged = merged.groupby(['ensemble','time']).mean()
merged = merged.reset_index().pivot(index='time',columns='ensemble',values=variable)
merged.to_csv(os.path.join(output_folder, "formatted_" + period + ".csv"))