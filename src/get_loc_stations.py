#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 16 17:55:13 2021

@author: a33272
"""


import numpy as np
import pandas as pd
import netCDF4

from scipy import interpolate
import matplotlib.pyplot as plt
import multiprocessing as mp

#year_start = 2018
#year_end = 2020
file_path = r'/test_data/WOD/csv_v01/'
timestamp = [(1900,2020)]
#timestamp = [(1981,1991),(1991,2001),(2001,2011),(2011,2021)]
platforms = ['apb', 'ctd', 'drb', 'gld', 'mbt', 'mrb', 'osd', 'pfl', 'sur', 'uor', 'xbt']
#platforms = ['ctd']

# Define an output queue
output = mp.Queue()

# define function
def process_platforms(year_start, year_end, platforms, file_path, output):
    columns = ['lat', 'lon', 'obs_num_all']
    df = pd.DataFrame(columns=columns)
    for year in range(year_start, year_end):
        for platform in platforms:
            url = 'https://www.ncei.noaa.gov/thredds-ocean/dodsC/ncei/wod/'+ str(year) + '/wod_' + platform + '_' + str(year) + '.nc'
        
            try:
                with netCDF4.Dataset(url) as nc:
                    d = {'lat': nc.variables['lat'][:], 
                         'lon': nc.variables['lon'][:],  
                         }
    
                    d['obs_num_all'] = [1] * len(d['lat'])
                    df_dum = pd.DataFrame(columns=columns, data=d)
                    df = pd.concat([df, df_dum])
                    df = df.groupby(['lon', 'lat'])['obs_num_all'].sum().reset_index()
                    print (str(year) + '-' + platform)
            except Exception:
                pass  # or you could use 'continue'

    output.put(df.to_csv(file_path + 'wod_' + str(year_start) + '-' + str(year_end-1) + '_stations.csv', index=False))
    return df
    
# Setup a list of processes that we want to run
#timestamp = list(range(year_start,year_end))
processes = [mp.Process(target=process_platforms, args=(year[0], year[1], platforms, file_path, output)) for year in timestamp]

# Run processes
for p in processes:
    p.start()

# Exit the completed processes
for p in processes:
    p.join()

# Get process results from the output queue
results = [output.get() for p in processes]

print(results)    
