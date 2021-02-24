#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 16 19:51:57 2021

@author: a33272
"""
import numpy as np
import pandas as pd
import netCDF4
import multiprocessing as mp


file_path = r'/test_data/WOD/csv_v01/'

timestamp = [(2011,2021)]
platforms = ['apb', 'ctd', 'drb', 'gld', 'mbt', 'mrb', 'osd', 'pfl', 'sur', 'uor', 'xbt']
vars_nc_list = ['Temperature_row_size', 'Salinity_row_size', 'Oxygen_row_size', 
                'Chlorophyll_row_size', 'pH_row_size', 'Phosphate_row_size', 'Silicate_row_size', 
                'Nitrate_row_size', 'Alkalinity_row_size', 'tCO2_row_size', 'CFC11_row_size', 
                'CFC12_row_size', 'CFC113_row_size', 'DeltaC14_row_size', 'Tritium_row_size', 
                'Helium_row_size', 'DeltaHe3_row_size', 'Neon_row_size', 'DeltaC13_row_size',
                'pCO2_row_size', 'Oxy18_row_size', 'Pressure_row_size']
#platforms = ['apb','ctd', 'xbt']
#vars_nc_list = ['Salinity_row_size','Oxygen_row_size']
#timestamp = [(2018,2019),(2019,2020)]

# Define an output queue
output = mp.Queue()

# define function
def process_platforms(var, year_start, year_end, platforms, file_path, output):
    columns = ['lat', 'lon', 'obs_num_all']
    df = pd.DataFrame(columns=columns)
    for year in range(year_start, year_end):
        for platform in platforms:
            url = 'https://www.ncei.noaa.gov/thredds-ocean/dodsC/ncei/wod/'+ str(year) + '/wod_' + platform + '_' + str(year) + '.nc'
        
            try:
                with netCDF4.Dataset(url) as nc:
                    d = {'lat': nc.variables['lat'][:], 
                         'lon': nc.variables['lon'][:], 
                         'var': nc.variables[var][:], 
                         }
                    
                    for i in range(0,len(d['var'])):
                         if isinstance(d['var'][i], np.ma.core.MaskedConstant):   
                            d['lat'][i] = np.nan
                            d['lon'][i] = np.nan
    
                    d['lat'] = [x for x in d['lat'] if str(x) != 'nan']
                    d['lon'] = [x for x in d['lon'] if str(x) != 'nan']
    
                    d['obs_num_all'] = [1] * len(d['lat'])
                    df_dum = pd.DataFrame(columns=columns, data=d)
                    df = pd.concat([df, df_dum])
                    df = df.groupby(['lon', 'lat'])['obs_num_all'].sum().reset_index()
                    print (str(year) + ' - ' + platform + ' - ' + var)
            except Exception:
                pass  # or you could use 'continue'

    output.put(df.to_csv(file_path + 'wod_' + str(year_start) + '-' + str(year_end-1) + var[:-9] + '_stations.csv', index=False))
    return df
    
# Setup a list of processes that we want to run

processes = [mp.Process(target=process_platforms, args=(var, timestamp[0][0], timestamp[0][1], platforms, file_path, output)) for var in vars_nc_list]

# Run processes
for p in processes:
    p.start()

# Exit the completed processes
for p in processes:
    p.join()

# Get process results from the output queue
results = [output.get() for p in processes]

print(results) 