#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 17 08:10:12 2021

@author: a33272
"""

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 14 14:19:54 2021

@author: a33272
"""

import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
from mpl_toolkits.basemap import Basemap
from mpl_toolkits.axes_grid1 import make_axes_locatable

def load_point_layer(file_path):
    df = pd.read_csv(file_path, header=0, sep=',', encoding = 'ISO-8859-1') 
    df = df[df['obs_num_all'] !=0]
    return df

def create_data_grid(df):
    df = round(df)
    df = df.groupby(['lon', 'lat'])['obs_num_all'].sum().reset_index()
    x = df.lon
    y = df.lat  
    gridx = np.linspace(-180, 180, 361)
    gridy = np.linspace(-90, 90, 181)
    grid, _, _ = np.histogram2d(x, y, bins=[gridx, gridy])
    return grid, df
    
def calculate_zero_coverage(grid):
    parcels_zeros = (grid == 0).sum()
    parcels_non_zeros = ((grid != 0).sum())
    all_parcels = parcels_zeros + parcels_non_zeros
    ocean_parcels = all_parcels*71/100
    percent_no_coverage = ((ocean_parcels - parcels_non_zeros)*100)/ocean_parcels
    return percent_no_coverage


def plot_gridded_data(df, grid, fig_path):   
    percent_no_coverage = calculate_zero_coverage(grid)
    
    fig = plt.figure()

    m = Basemap(llcrnrlon=-180.,llcrnrlat=-89.,urcrnrlon=180.,urcrnrlat=89.,\
            rsphere=(6378137.00,6356752.3142),\
            resolution='l',projection='cyl',\
            lat_0=40.,lon_0=-20.,lat_ts=20.)
    m.drawcoastlines(linewidth=0.25)
    m.fillcontinents(color='grey')
    t = df.obs_num_all
    lon, lat = m(df.lon, df.lat)  # transform coordinates
    
    cmap = mpl.colors.ListedColormap(['blue', 'cyan', 'green','yellow', 'orange', 'red'])
    
    plt.scatter(lon, lat, c=t, cmap=cmap, marker='s',linewidth=0.2, s=0.6,vmin=1, vmax=500) 
    #plt.xticks(lon)

#    plt.xlabel('Longitude [degrees]')
#    plt.ylabel('Latitude [degrees]')
#    plt.title('Number of observations per 1deg parcel')
    annotation = 'Percentage not covered: ' + str(round(percent_no_coverage,2)) + '%'
    plt.annotate(annotation, xy=(-179.9, -125), xycoords='data', annotation_clip=False)
    
    cmap = mpl.colors.ListedColormap(['white','blue', 'cyan', 'green', 'yellow', 'orange', 'red'])
#    cmap.set_over('red')
#    cmap.set_under('white')
    
    # define the bins and normalize
    #bounds = [0,1,100,1000,10000,20000,100000] # decade all vars
    #bounds = [0,1,2,5,10,100,1000,10000] # decade single vars
    #bounds = [0,1,10,100,1000,10000,100000,1000000] # 1900-2020
    #bounds = [0,1,10,100,200,500,1000] # decade single vars in depth layer
    bounds = [0,1,2,5,10,50,60,500] # decade stations
    norm = mpl.colors.BoundaryNorm(bounds, cmap.N)
    cbar = plt.colorbar(
        mpl.cm.ScalarMappable(cmap=cmap, norm=norm),
        #cax=ax,
        #boundaries=[-5] + bounds + [5],
        extend='max',
        extendfrac='auto',
        ticks=bounds,
        spacing='uniform',
        orientation='vertical',
        label='Number of observations',
    )
    
    #cbar=plt.colorbar(fraction=0.046, pad=0.04, label="Number of observations", spacing='proportional')
    cbar.ax.tick_params(labelsize=5)
    plt.show()
    fig.savefig(fig_path, bbox_inches='tight', dpi=800)


file_path = '/test_data/WOD/csv_v01/wod_2018_Oxygen_z[0, 1010]_stations.csv'
df = load_point_layer(file_path)
grid, df = create_data_grid(df)
fig_name = file_path.split('/')[-1][:-4]
plot_gridded_data(df, grid, '/test_data/WOD/figs/'+fig_name)


