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

data={'username':'YOUR_USERNAME',
      'password':'YOUR_PASSWORD'} # create an account here: https://services.ceda.ac.uk/cedasite/register/info/


output_folder = os.path.join("C:\\","Users","bdobson","Downloads","")
start_year = 1975
end_year = 1980
grid_scale = 5 # km
period = 'day' #'day', 'mon' or 'ann'
variable = 'rainfall' #tested 'rainfall' and 'tas' (temperature)


"""Website information
Give site and file urls
Create dates and file names
"""

site_url = 'https://auth.ceda.ac.uk/account/signin/' # login page

file_url_base = 'http://dap.ceda.ac.uk/thredds/fileServer/badc/ukmo-hadobs/data/insitu/MOHC/HadOBS/HadUK-Grid/v1.0.1.0/' # hadUK data base address
version = 'v20190808' # the latest release I suppose

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
