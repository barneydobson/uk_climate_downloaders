# -*- coding: utf-8 -*-
"""
Created on Thu Feb 13 12:51:35 2020

@author: bdobson
"""

# -*- coding: utf-8 -*-
"""
This is a script to download hadUK data, written by Barnaby Dobson, 5th, Dec, 2019
"""

import os
from tqdm import tqdm
import haduk_downloader_functions


"""User data
This includes, username, password, output location and details for the download
"""

output_folder = os.path.join("C:\\","Users","Barney","Downloads","")
start_year = 2014
end_year = 2016 # Up to but not including the end_year

data={'username':'ceh_username',
      'password':'ceh_password'} 

"""Website information
Give site and file urls
Create dates and file names
"""
file_name = 'Historic_daily_PET_McGuinness-Bordne_'
file_root = 'https://catalogue.ceh.ac.uk/datastore/eidchub/17b9c4f7-1c30-4b6f-b2fe-f7780159939c/DailyPET/' 
site_url = r'https://eip.ceh.ac.uk/sso/login/'
dates_series = haduk_downloader_functions.petDateseries(start_year,end_year)


#seems to need a new client each time
s = haduk_downloader_functions.startCehClient(data,site_url)

"""Iterate over dates
"""

for period_text in tqdm(dates_series):

        
    #Format url
    file_url = file_root + file_name + period_text + '.nc'
    #Make request
    req = s.get(file_url)
    
    #Write request
    with open(os.path.join(output_folder,file_name + period_text + '.nc'),'wb') as f:
        f.write(req.content)
