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

data={'username':'USERNAME',
      'password':'PASSWORD'} # create an account here: https://services.ceda.ac.uk/cedasite/register/info/


output_folder = os.path.join("C:\\", "Users", "bdobson", "OneDrive - Imperial College London", "students", "msc", "misc", "patrick","")
start_year = 2011
end_year = 2019 # Up to but not including the end_year
grid_scale = 25 # km
period = 'mon' #'day', 'mon' or 'ann'
variable = 'tasmax' #tested 'rainfall' and 'tas' (temperature)


"""Website information
Give site and file urls
Create dates and file names
"""
site_url = 'https://auth.ceda.ac.uk/account/signin/' # login page

file_url_base = 'http://dap.ceda.ac.uk/badc/ukmo-hadobs/data/insitu/MOHC/HadOBS/HadUK-Grid/v1.0.2.1/' # hadUK data base address
version = 'v20200731' # the latest release I suppose

dates_series = haduk_downloader_functions.createDateseries(period,start_year,end_year)

file_name = haduk_downloader_functions.getFilename(variable,grid_scale,period)

file_location = haduk_downloader_functions.getFileweblocation(grid_scale,variable,period,version)


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

for period_text in tqdm(dates_series):
    file_url = file_url_base + file_location + period_text + '.nc'
    req = s.get(file_url)
    with open(os.path.join(output_folder,file_name + period_text + '.nc'),'wb') as f:
        f.write(req.content)
