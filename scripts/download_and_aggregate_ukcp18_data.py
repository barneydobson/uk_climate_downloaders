# -*- coding: utf-8 -*-
"""
This is a script to download UKCP18 data, written by Barnaby Dobson, 21st, July, 2021
Browse available data at: https://data.ceda.ac.uk/badc/ukcp18/data
"""

import os
from tqdm import tqdm
import haduk_downloader_functions
import xarray as xr
import geopandas as gpd
import pandas as pd

"""User data
This includes, username, password, output location and details for the download
"""
data={'username':'YOUR_USERNAME',
      'password':'YOUR_PASSWORD'} # create an account here: https://services.ceda.ac.uk/cedasite/register/info/

#shp is the shape that you want to aggregate the data to
shp_fid = os.path.join("C:\\", "Users", "bdobson", "OneDrive - Imperial College London", "maps", "wrz", "wrz.geojson")
shp = gpd.read_file(shp_fid) # Assuming shp is BNG and has crs defined!!!
name = 'RZ_ID' # Name that you want to use to label each sub-area (each geometry in shp should have a unique 'name')
output_folder = os.path.join("C:\\","Users","bdobson","Documents","data","ukcp18", "")
start_year = 1980
end_year = 2080 # Up to but not including the end_year
resolution = '12km' # can be '12km', 'country', 'region', 'river' - or whatever is available for that model
variables = ["tas", "sfcWind", "hurs", "rss", "rls", "pr"] # https://www.metoffice.gov.uk/binaries/content/assets/metofficegovuk/pdf/research/ukcp/ukcp18-guidance-data-availability-access-and-formats.pdf
model = 'rcm' # can be 'rcm', 'gcm', 'cpm'
scenario = 'rcp85' #
version = 'latest'
ensembles = ['01',
             '04',
             '05',
             '06',
             '07',
             '08',
             '09',
             '10',
             '11',
             '12',
             '13',
             '15'] # e.g. what is available here https://data.ceda.ac.uk/badc/ukcp18/data/land-rcm/uk/12km/rcp85
area = 'uk' # can be 'uk', 'eur' or 'global', but depends on the model
#NOTE: if you use an area other than UK, you will have to be careful about the spatial aggregation since these data presumably do not use BNG
period = 'day' #'day', 'mon' or 'ann' (or '1hr' for 'cpm' model)
#NOTE: the dateseries will need to be customised if using period other than 'day'


"""Website information
Give site and file urls
Create dates and file names
"""
site_url = 'https://auth.ceda.ac.uk/account/signin/' # login page

file_url_base = 'https://dap.ceda.ac.uk/badc/ukcp18/data/land-'

date_series = haduk_downloader_functions.createDateseries('decade', start_year, end_year)

"""Login
"""
try:
   s = haduk_downloader_functions.startClient(data,site_url)
except:
    cert_location = 'qvevsslg3.pem' # Available from https://www.quovadisglobal.com/QVRepository/DownloadRootsAndCRL.aspx and translated to .pem following instructions in https://incognitjoe.github.io/adding-certs-to-requests.html
    haduk_downloader_functions.addCertificate(cert_location)
    s = haduk_downloader_functions.startClient(data,site_url)


"""Iterate over years
"""
p_e_v = [(x, y, z) for x in date_series for y in ensembles for z in variables]
dfs = []
for date, ensemble, variable in tqdm(p_e_v):
    period_text = '_'.join([variable,
                            scenario,
                            'land-' + model,
                            area,
                            resolution,
                            ensemble,
                            period,
                            date])
    file_url = file_url_base + '/'.join([model,
                                         area,
                                         resolution,
                                         scenario,
                                         ensemble,
                                         variable,
                                         period,
                                         version,
                                         '']) + period_text + '.nc'
    req = s.get(file_url)
    
    fid = os.path.join(output_folder,period_text + '.nc')
    with open(fid,'wb') as f:
        f.write(req.content)
    
    with xr.open_dataset(fid) as ds:
        df = ds[variable].to_dataframe()[variable]
    
    #Remove file
    os.remove(fid)
    
    #Format dataframe
    df = df.reset_index()

    df['variable'] = variable
    df = df.rename(columns={variable : 'value',
                            'ensemble_member' : 'ensemble',
                            'projection_y_coordinate' : 'y',
                            'projection_x_coordinate' : 'x'})
    
    #Join with shp and groupby over shp
    points = df[['x','y']].drop_duplicates()
    points = gpd.GeoDataFrame(points, geometry = gpd.points_from_xy(x = points.x, y = points.y), crs = shp.crs)
    points = gpd.sjoin(points, shp[[name,'geometry']])
    
    df = pd.merge(df, points[['x','y',name]], on = ['x','y'])
    df = df.groupby(['ensemble', 'time', 'variable', name]).mean()
    dfs.append(df)

dfs = pd.concat(dfs)
dfs.to_parquet(output_folder + '_'.join([model, resolution, scenario]) + '.gzip')
