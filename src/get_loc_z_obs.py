#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  9 15:52:02 2020

@author: a33272
"""
import numpy as np
import numpy.ma as ma
import pandas as pd
import netCDF4

from scipy import interpolate
import matplotlib.pyplot as plt

year = 2018
month = []
var_qc = True
z_layer = [[0,200],[200,1000],[1000,4000],[4000,6000],[6000,11000]]
#z_layer = [[4000,6000]]
var_to_process = ['pCO2']
var_sufix = '_row_size'
qc_sufix = '_WODprofileflag'


file_out_path = r'/test_data/WOD/csv_v01/'

platforms = ['apb', 'ctd', 'drb', 'gld', 'mbt', 'mrb', 'osd', 'pfl', 'sur', 'uor', 'xbt']
#platforms = ['ctd']


def clean_vars_nc_list(v, vars_nc_list):
    remove_list = ['z_row_size', 'Primary_Investigator_row_size', 'Latitude_row_size', 'Longitude_row_size', 'JulianDay_row_size']
    if '_row_size' in v:
        vars_nc_list.append(v)
    [vars_nc_list.remove(x) for x in remove_list if x in vars_nc_list]
    return vars_nc_list

def count_observations_in_depth_range(cast_bins, var_obs_cast, obs_num_count_station, z):
    if any(i in cast_bins for i in z):
        if var_obs_cast != 0:
            indices = [i for i, x in enumerate(cast_bins) if z[0] <= x <= z[1]]
            observations = len(indices)
            obs_num_count_station.append(observations)
        else:
            obs_num_count_station.append(0)
    else: 
        obs_num_count_station.append(0)
    return obs_num_count_station

def process_platform(var_to_process, vars_nc_list, nc, year, platform, z):
    if var_to_process[0]:
        vars_nc_list.append(var_to_process[0] + var_sufix)
    else:
        for v in nc.variables:
            vars_nc_list = clean_vars_nc_list(v, vars_nc_list)
    
    
    d = {'lat': nc.variables['lat'][:], 
         'lon': nc.variables['lon'][:], 
         }
    stations_num = len(d['lat'])
    z_depth = np.round(nc.variables['z'][:]) # total depth measurements
    z_obs = np.round(nc.variables['z_row_size'][:]) # number of depth observations per cast
    obs_num_count_varnc = []
    
    for var_nc in vars_nc_list:
        if var_qc == True:
            var_obs_qc = nc.variables[var_to_process[0] + qc_sufix][:]
            var_obs = nc.variables[var_nc][:]
            for x in range(len(var_obs)):
                if var_obs_qc[x] != 0:
                   var_obs[x]=0 
        else:
            var_obs = nc.variables[var_nc][:] # number of variable observations per cast
        var_obs = var_obs.filled(fill_value=0)                    
        obs_num_count_station = []
        cast_idx = 0
        
        for station_idx in range(stations_num):
            print(''.join(['   ' + str(station_idx) + '  -  ' + var_nc]))                    
            #if z in z_depth[cast_idx:cast_idx+z_obs[station_idx]]:
            cast_bins = z_depth[cast_idx:cast_idx+z_obs[station_idx]]
            cast_z_max = max(cast_bins)
            cast_z_min = min(cast_bins)
            if z[1] < cast_z_max or z[0] > cast_z_min:
                obs_num_count_station = count_observations_in_depth_range(cast_bins, var_obs[station_idx], obs_num_count_station, z)
            else: 
                obs_num_count_station.append(0)
            cast_idx = cast_idx + z_obs[station_idx]
        obs_num_count_varnc.append(obs_num_count_station)  
    d['obs_num_all'] = [sum(x) for x in zip(*obs_num_count_varnc)] 
    
    return d





#for year in range(year_start, year_start+1):
columns = ['lat', 'lon', 'obs_num_all']
for z in z_layer:

    df = pd.DataFrame(columns=columns)
    for platform in platforms:
        try: 
            url = 'https://www.ncei.noaa.gov/thredds-ocean/dodsC/ncei/wod/'+ str(year) + '/wod_' + platform + '_' + str(year) + '.nc'
            vars_nc_list = []
    
            with netCDF4.Dataset(url) as nc:   
                print (str(year) + '  -  ' + platform + '  -  ' + str(z) + ' START')
                d = process_platform(var_to_process, vars_nc_list, nc, year, platform, z)

            
            #d['platform'] = [platform] * stations_num
            df_platform= pd.DataFrame(columns=columns, data=d)
            df_platform = df_platform[df_platform['obs_num_all'] !=0]
            frames = [df, df_platform]
            df = pd.concat(frames)            
            df = df.groupby(['lon', 'lat'])['obs_num_all'].sum().reset_index()

            print (str(year) + '  -  ' + platform + '  -  ' + str(z) + ' END')
        except Exception as e:
            print ('Ops!!! AN ERROR OCCURRED YOU DONKEY:   ' + str(e))
    
    if var_qc == True:
        fname_sufix = '_z' + str(z) + '_' + str(var_to_process[0] + '_qc')   
    else:
        fname_sufix = '_z' + str(z) + '_' + str(var_to_process[0])
    df.to_csv(file_out_path + 'wod_' + str(year) + fname_sufix + '.csv', index=False)
#df.to_csv(file_out_path + 'wod_' + str(year_start) + '-' + str(year_end-1) + var_sufix + '.csv', index=False)
    
    
    
### ----------------------------------------------------------------------------------------------------------------------------------
### Execute the code
#from multiprocessing import Process
#
##if __name__ == '__main__':
##    p = Process(target=process_data, args=(year_start, year_end, platforms,))
##    p.start()
##    p.join()
#    
#from multiprocessing import Pool
#p = Pool(5)
#
#with p:
#    p.map(process_data(year_start, year_end, platforms))    
