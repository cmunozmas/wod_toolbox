#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  9 15:52:02 2020

@author: a33272
"""
import numpy as np
import pandas as pd
import netCDF4

from scipy import interpolate
import matplotlib.pyplot as plt

year_start = 2018
year_end = 2019
file_path = r'/test_data/WOD/csv_v01/'
var_sufix = ''

platforms = ['apb', 'ctd', 'drb', 'gld', 'mbt', 'mrb', 'osd', 'pfl', 'sur', 'uor', 'xbt']
#platforms = ['apb']
vars_nc_list = ['Temperature_row_size', 'Salinity_row_size', 'Oxygen_row_size', 
                'Chlorophyll_row_size', 'pH_row_size', 'Phosphate_row_size', 'Silicate_row_size', 
                'Nitrate_row_size', 'Alkalinity_row_size', 'tCO2_row_size', 'CFC11_row_size', 
                'CFC12_row_size', 'CFC113_row_size', 'DeltaC14_row_size', 'Tritium_row_size', 
                'Helium_row_size', 'DeltaHe3_row_size', 'Neon_row_size', 'DeltaC13_row_size',
                'pCO2_row_size', 'Oxy18_row_size', 'Pressure_row_size']

#vars_df_list = ['temp_obs', 'psal_obs', 'doxy_obs', 'chla_obs', 'ph_obs', 'phos_obs', 
#                'silc_obs', 'nitr_obs', 'alkt_obs', 'tco2_obs', 'cfc11_obs', 'cfc12_obs', 
#                'cfc113_obs', 'dc14_obs', 'trit_obs', 'heli_obs', 'dhe3_obs', 'neon_obs',
#                'dc13_obs', 'pco2_obs', 'ox18_obs', 'pres_obs']

#vars_nc_list = ['Oxygen_row_size']

#vars_df_list = ['doxy_obs']
#columns = ['lat', 'lon', 'date'] + vars_df_list + ['obs_num_all', 'platform']
columns = ['lat', 'lon', 'obs_num_all']

df = pd.DataFrame(columns=columns)

cast_num_count = []
for year in range(year_start, year_end):
    for platform in platforms:
        url = 'https://www.ncei.noaa.gov/thredds-ocean/dodsC/ncei/wod/'+ str(year) + '/wod_' + platform + '_' + str(year) + '.nc'
    
        try:
            with netCDF4.Dataset(url) as nc:
                d = {'lat': nc.variables['lat'][:], 
                     'lon': nc.variables['lon'][:], 
                     'date': nc.variables['date'][:], 
                     }
                obs_num_count = []
                #for var_nc, var_df in zip(vars_nc_list, vars_df_list):
                for var_nc in vars_nc_list:
                    try:
                        var_obs = nc.variables[var_nc][:]
                        var_obs[var_obs.mask] = 0
                        var_obs_filter = [i for i in var_obs if i != 0]
                        cast_num_count.append(len(var_obs_filter))
                    except:
                        var_obs = 0                

                    obs_num_count.append(var_obs)
                    
                d['obs_num_all'] = sum(obs_num_count)
                
                d['platform'] = [platform] * len(d['lat'])
                df_dum = pd.DataFrame(columns=columns, data=d)
                df = pd.concat([df, df_dum])
                df = df.groupby(['lon', 'lat'])['obs_num_all'].sum().reset_index()
                print (str(year) + '-' + platform)
        except Exception:
            pass  # or you could use 'continue'
casts_total = sum(cast_num_count)
df.to_csv(file_path + 'wod_' + str(year_start) + '-' + str(year_end-1) + var_sufix + '.csv', index=False)
    
    
