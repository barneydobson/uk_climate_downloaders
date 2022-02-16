# -*- coding: utf-8 -*-
"""
Created on Wed Feb 16 13:48:29 2022

@author: bdobson
"""
import os
import pandas as pd
import numpy as np
import math
data_folder = os.path.join("C:\\", "Users", "bdobson", "Documents", "data", "haduk")
output_folder = os.path.join("C:\\", "Users", "bdobson", "Documents", "GitHub", "wsimod", "projects", "fine_catchments", "data", "enfield", "raw")
labels = ['tas','hurs','sun','sfcWind','tasmin','tasmax']
timestep = ['mon','mon','mon','mon','day','day']
dates = '2006_2007'
grid = '1_km'

full_df = []
for variable, time in zip(labels, timestep):
    df = pd.read_csv(os.path.join(data_folder, "_".join([variable,
                                                         dates,
                                                         time,
                                                         grid,
                                                         "aggregated.csv"
                                                        ])))
    df['date'] = pd.to_datetime(df.date)
    
    if variable == 'tas':
        tas_lagged = df.copy().iloc[1:]
        tas_lagged['date'] = df.iloc[0:-1]['date'].values
        tas_lagged = tas_lagged.set_index('date')
        tas_lagged.loc[df['date'].iloc[-1]] = df.drop('date', axis = 1).iloc[-1]
        tas_lagged = tas_lagged.rename({tas_lagged.index[0] : tas_lagged.index[0].replace(day=1),
                                        tas_lagged.index[-1] : pd.to_datetime(pd.Period(tas_lagged.index[-1],freq='M').end_time.date())})
        tas_lagged = tas_lagged.resample('1d').ffill()
        tas_lagged['variable'] = 'tas_lagged'
        full_df.append(tas_lagged)
    #TODO: something for leap years
    df = df.set_index('date')
    if time == 'mon':
        df = df.rename({df.index[0] : df.index[0].replace(day=1),
                        df.index[-1] : pd.to_datetime(pd.Period(df.index[-1],freq='M').end_time.date())})
        df = df.resample('1d').ffill()
    df['variable'] = variable
    full_df.append(df)
    

full_df = pd.concat(full_df)
gb = full_df.groupby('variable')

def retrieve(v):
    return gb.get_group(v).drop('variable', axis=1)

#Parameters
altitude = 20 # [m]
z = 10 # [m] measured height of surface wind data
latitude = 51.752022 # [degree N]
cp = 1.013 * 1e-3 # [MJ/kg/dgree C]
lambda_ = 2.45 # [MJ/kg]
epsilon = 0.622 # [-]
sigma = 4.903 * 1e-9 # [MJ K-4 m-2 d-1]
Gsc = 0.0820 # [MJ m-2 min-1]
P = 101.3 * ((293 - 0.0065 * altitude)/293) ** 5.26 # [kPa]


#Calculation
delta = 4098 * (0.6108 * np.exp(17.27  * retrieve('tas')/(retrieve('tas') + 237.3))) / (retrieve('tas') + 237.3) ** 2 # [kPa/degree C]
gama = cp * P / (epsilon * lambda_) # [kPa/degree C]

e_Tmax = 0.6108 * np.exp(17.27 * retrieve('tasmax') / (retrieve('tasmax') + 237.3)) # [kPa]
e_Tmin = 0.6108 * np.exp(17.27 * retrieve('tasmin') / (retrieve('tasmin') + 237.3)) # [kPa]
es = (e_Tmax + e_Tmin) / 2
ea = es * retrieve('hurs') / 100

J = np.array(retrieve('tas').index.dayofyear)
dr = 1 + 0.033 * np.cos(2 * np.pi / 365 * J) # inverse relative distance Earth-Sun
delta_ = 0.409 * np.sin(2 * np.pi / 365 * J - 1.39) # [rad] solar declination
fai = latitude * np.pi / 180 # [rad] latitude
ws = np.arccos(-np.tan(fai) * np.tan(delta_)) # [rad] sunset hour angle
Ra = 24 * 60 / np.pi * Gsc * dr * (ws * np.sin(fai) * np.sin(delta_) + np.cos(fai) * np.cos(delta_) * np.sin(ws)) # [MJ/m2/d] extra-terrestrial radiation
N = 24 * ws / np.pi # [hours] maximum sunshine duration

n = retrieve('sun').div(retrieve('sun').index.daysinmonth, axis = 0)
u2 = retrieve('sfcWind') * 4.87 / math.log(67.8 * z - 5.42)

ET0 = []
for col in n.columns:
    
    Rs = (0.25 + 0.5 * n[col] / N) * Ra # [MJ/m2/d] active incoming shortwave radiation
    Rso = (0.75 + 2 * 1e-5 * altitude) * Ra # [MJ/m2/d]
    Rns = (1 - 0.23) * Rs # [MJ/m2/d]
    Rnl = sigma * ((retrieve('tasmax')[col] + 273.16) + (retrieve('tasmin')[col] + 273.16)) / 2 * (0.34 - 0.14 * np.sqrt(ea[col])) * (1.35 * Rs / Rso - 0.35) # [MJ/m2/d]
    Rn = Rns - Rnl # [MJ/m2/d]
    G = 0.14 * (retrieve('tas')[col] - retrieve('tas_lagged')[col]) # [MJ/m2/d]
    
    ET0.append((0.408 * delta[col] * (Rn - G) + gama * 900 / (retrieve('tas')[col] + 273) * u2[col] * (es[col] - ea[col])) / (delta[col] + gama * (1 + 0.34 * u2[col]))) # [mm]

ET0 = pd.concat(ET0, axis = 1)
ET0.to_csv(os.path.join(output_folder, "_".join(["et0",
                                                         dates,
                                                         time,
                                                         grid,
                                                         "aggregated.csv"
                                                        ])))