# -*- coding: utf-8 -*-
"""
Created on Thu Dec  5 09:03:38 2019

@author: bdobson
"""
import requests
import pandas as pd
import certifi

def startCehClient(data,site_url):
    s = requests.Session()
    r = s.post(site_url, data = data)
    return s

def startClient(data,site_url):
    """Log in to the ceda data store
    """
    s = requests.Session()
    s.get(site_url)
    csrftoken = s.cookies['csrftoken']
    data['csrfmiddlewaretoken']=csrftoken
    r = s.post(site_url,data=data,headers=dict(Referer=site_url))
#    print(r.content)
    return s

def petDateseries(start_year,end_year):
    return [str(x) for x in list(range(start_year,end_year+1))]

def createDateseries(period,start_year,end_year):
    

    if period == 'day':
        end_year += 1
        dates = pd.date_range(pd.to_datetime(start_year*100+1,format='%Y%m'),
                              pd.to_datetime(end_year*100+1,format='%Y%m'),
                              freq='m')
        dates_series = dates.strftime('%Y%m') + '01-' + dates.strftime('%Y%m%d')
    elif period == 'decade':
        # weird ukcp18 formats
        dates1 = pd.date_range(pd.to_datetime(start_year*10000+1201,format='%Y%m%d'),
                              pd.to_datetime(end_year*10000+1201,format='%Y%m%d'),
                              freq='10Y') - pd.DateOffset(days = 30)
        dates2 = pd.date_range(pd.to_datetime((start_year+10)*10000+1201,format='%Y%m%d'),
                              pd.to_datetime((end_year+10)*10000+1201,format='%Y%m%d'),
                              freq='10Y') - pd.DateOffset(days = 31)
        dates_series = dates1.strftime('%Y%m%d') + '-' + dates2.strftime('%Y%m%d')
    else:
        end_year += 1
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
    cafile = certifi.where()
    with open(cert_location,'rb') as infile: 
        customca = infile.read()
    with open(cafile,'ab') as outfile:
        outfile.write(customca)
