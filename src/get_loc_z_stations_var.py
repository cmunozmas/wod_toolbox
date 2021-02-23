#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 18 05:03:03 2021

@author: a33272
"""

import numpy as np
import pandas as pd
import netCDF4
import multiprocessing as mp


file_path = r'/test_data/WOD/csv_v01/'
z_layer = [[0,200],[200,1000],[1000,4000],[4000,6000],[6000,11000]]

year = 2018
platforms = ['apb', 'ctd', 'drb', 'gld', 'mbt', 'mrb', 'osd', 'pfl', 'sur', 'uor', 'xbt']
vars_nc_list = ['Temperature_row_size', 'Salinity_row_size', 'Oxygen_row_size', 
                'Chlorophyll_row_size', 'pH_row_size', 'Phosphate_row_size', 'Silicate_row_size', 
                'Nitrate_row_size', 'Alkalinity_row_size', 'tCO2_row_size', 'CFC11_row_size', 
                'CFC12_row_size', 'CFC113_row_size', 'DeltaC14_row_size', 'Tritium_row_size', 
                'Helium_row_size', 'DeltaHe3_row_size', 'Neon_row_size', 'DeltaC13_row_size',
                'pCO2_row_size', 'Oxy18_row_size', 'Pressure_row_size']
#z_layer = [[1000,4000]]
#platforms = ['ctd']
#vars_nc_list = ['Oxygen_row_size']
#timestamp = [(2018,2019),(2019,2020)]

# Define an output queue
output = mp.Queue()

# define function
def process_platforms(var, year, z_layer, platforms, file_path, output):
#    var=vars_nc_list[0]
    var_name = var[:-9]
    columns = ['lat', 'lon', 'obs_num_all']
    
    for z in z_layer:
        df = pd.DataFrame(columns=columns)
        for platform in platforms:
            url = 'https://www.ncei.noaa.gov/thredds-ocean/dodsC/ncei/wod/'+ str(year) + '/wod_' + platform + '_' + str(year) + '.nc'
        
            try:
                with netCDF4.Dataset(url) as nc:
                    d = {'lat': nc.variables['lat'][:], 
                         'lon': nc.variables['lon'][:], 
                         'var': nc.variables[var][:], 
                         }
                    
                    
                    stations_num = len(d['lat'])
                    z_depth = np.round(nc.variables['z'][:]) # total depth measurements
                    z_obs = np.round(nc.variables['z_row_size'][:]) # number of depth observations per cast                    
                    cast_idx = 0
                    num_count_station = []
                    for station_idx in range(stations_num):
                        #print(''.join(['   ' + str(station_idx) + '  -  ' + var]))                    
                        cast_bins = z_depth[cast_idx:cast_idx+z_obs[station_idx]]
                        cast_z_max = max(cast_bins)
                        cast_z_min = min(cast_bins)

                        if z[1] < cast_z_max:
                            if d['var'][station_idx]:
                                num_count_station.append(1)
                            else: 
                                num_count_station.append(0)
                        else: 
                            num_count_station.append(0)
                        cast_idx = cast_idx + z_obs[station_idx]
      
                    for i in range(0,len(num_count_station)):
                        if num_count_station[i] == 0:   
                            d['lat'][i] = np.nan
                            d['lon'][i] = np.nan
    
                    d['lat'] = [x for x in d['lat'] if str(x) != 'nan']
                    d['lon'] = [x for x in d['lon'] if str(x) != 'nan']
    
                    d['obs_num_all'] = [1] * len(d['lat'])
                    df_dum = pd.DataFrame(columns=columns, data=d)
                    df = pd.concat([df, df_dum])
                    df = df.groupby(['lon', 'lat'])['obs_num_all'].sum().reset_index()
                    print (str(year) + ' - ' + platform + ' - ' + var_name + '- z' + str(z))
            except Exception:
                pass  # or you could use 'continue'
    
        df.to_csv(file_path + 'wod_' + str(year) + '_' + var_name + '_z' + str(z) + '_stations.csv', index=False)
    return df
    
# Setup a list of processes that we want to run

processes = [mp.Process(target=process_platforms, args=(var, year, z_layer, platforms, file_path, output)) for var in vars_nc_list]

# Run processes
for p in processes:
    p.start()

# Exit the completed processes
for p in processes:
    p.join()

# Get process results from the output queue
results = [output.get() for p in processes]

#print(results) 
print('process completed')