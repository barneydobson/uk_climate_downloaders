# -*- coding: utf-8 -*-
"""
Created on Thu Dec  5 09:03:38 2019

@author: bdobson
"""
import requests
import pandas as pd
import certifi

def startClient(data,site_url):
    """Log in to the ceda data store
    """
    s = requests.Session()
    s.get(site_url)
    csrftoken = s.cookies['csrftoken']
    data['csrfmiddlewaretoken']=csrftoken
    r = s.post(site_url,data=data,headers=dict(Referer=site_url))
#    r = s.post(site_url,json=data,headers=dict(Referer=site_url),verify=False)
#    print(r.content)
    return s

def createDateseries(period,start_year,end_year):
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
    return dates_series

def getFilename(variable,grid_scale,period):
    return '%s_hadukgrid_uk_%dkm_%s_' % (variable,
                                         grid_scale,
                                         period)

def getFileweblocation(grid_scale,variable,period,version):
    return '%dkm/%s/%s/%s/%s' % (grid_scale,
                                         variable,
                                         period,
                                         version,
                                         getFilename(variable,grid_scale,period))
    
def addCertificate(cert_location):
    #Alternatively you can add 'verify=False' in the post command in startClient() - but this is not secure
    cafile = certifi.where()
    with open(cert_location,'rb') as infile: 
        customca = infile.read()
    with open(cafile,'ab') as outfile:
        outfile.write(customca)