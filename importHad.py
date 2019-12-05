# -*- coding: utf-8 -*-
"""
This is a script to download hadUK data, written by Barnaby Dobson, 5th, Dec, 2019
"""

import requests
import certifi
import os
import pandas as pd
from tqdm import tqdm

def startClient(data,site_url):
    """Log in to the ceda data store
    """
    s = requests.Session()
    s.get(site_url)
    csrftoken = s.cookies['csrftoken']
    data['csrfmiddlewaretoken']=csrftoken
    r = s.post(site_url,data=data,headers=dict(Referer=site_url))
#    r = s.post(site_url,json=data,headers=dict(Referer=site_url),verify=False)
    print(r.content)
    return s

"""User data
This includes, username, password, output location and details for the download
"""

data={'username':'YOUR_USERNAME',
      'password':'YOUR_PASSWORD'} # create an account here: https://services.ceda.ac.uk/cedasite/register/info/


output_folder = os.path.join("C:\\","Users","bdobson","Downloads","")
start_year = 1960
end_year = 1961
grid_scale = 25 # km
period = 'day' #'day', 'mon' or 'ann'
variable = 'rainfall' #tested 'rainfall' and 'tas' (temperature)


"""Website information
Give site and file urls
Create dates and file names
"""

site_url = 'https://auth.ceda.ac.uk/account/signin/' # login page

file_url_base = 'http://dap.ceda.ac.uk/thredds/fileServer/badc/ukmo-hadobs/data/insitu/MOHC/HadOBS/HadUK-Grid/v1.0.1.0/' # hadUK data base address
version = 'v20190808' # the latest release I suppose

if period == 'day':
    dates = pd.date_range(pd.to_datetime(start_year*100+1,format='%Y%m'),
                          pd.to_datetime(end_year*100+1,format='%Y%m'),
                          freq='m')
    dates_series = dates.strftime('%Y%m') + '01-' + dates.strftime('%Y%m%d')
else:
    dates = pd.date_range(pd.to_datetime(start_year*100+1,format='%Y%m'),
                          pd.to_datetime(end_year*100+1,format='%Y%m'),
                          freq='Y')
    dates_series = dates.strftime('%Y') + '01-' + dates.strftime('%Y%m')

file_name = '%s_hadukgrid_uk_%dkm_%s_' % (variable,
                                         grid_scale,
                                         period)
file_location = '%dkm/%s/%s/%s/%s' % (grid_scale,
                                         variable,
                                         period,
                                         version,
                                         file_name)


"""Login
"""

try:
   s = startClient(data,site_url)
except:
    #Alternatively you can add 'verify=False' in the post command in startClient() - but this is not secure
    cafile = certifi.where()
    with open('qvevsslg3.pem','rb') as infile: #Available from https://www.quovadisglobal.com/QVRepository/DownloadRootsAndCRL.aspx and translated to .pem following instructions in https://incognitjoe.github.io/adding-certs-to-requests.html
        customca = infile.read()
    with open(cafile,'ab') as outfile:
        outfile.write(customca)
    s = startClient(data,site_url)


"""Iterate over years
"""

for period_text in tqdm(dates_series):
    file_url = file_url_base + file_location + period_text + '.nc'
    req = s.get(file_url)
    with open(os.path.join(output_folder,file_name + period_text + '.nc'),'wb') as f:
        f.write(req.content)
