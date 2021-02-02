#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  9 15:52:02 2020

@author: a33272
"""
import numpy as np
import pandas as pd
import netCDF4
import xarray as xr
from scipy import interpolate
import matplotlib.pyplot as plt

year_start = 2020
year_end = 2021

file_path = r'/test_data/WOD/'
platforms = ['drb']
#platforms = ['apb', 'ctd', 'drb', 'gld', 'mbt', 'mrb', 'osd', 'pfl', 'uor', 'xbt']

columns = ['institute']

#df = pd.DataFrame(columns=columns)


for year in range(year_start, year_end):
    for platform in platforms:
        url = 'https://www.ncei.noaa.gov/thredds-ocean/dodsC/ncei/wod/'+ str(year) + '/wod_' + platform + '_' + str(year) + '.nc'
    
        try:
            DS = xr.open_dataset(url)
            da = DS.Institute
            da = da.astype(str)
            da_list = da.tolist()   
            df = da.to_pandas()
            #df=df.drop_duplicates()

            print (str(year) + '-' + platform)
        except Exception:
            pass  # or you could use 'continue'
    #df.to_csv(file_path + 'wod_' + str(year_start) + '-' + str(year_end-1) + '.csv', index=False)
    
    


