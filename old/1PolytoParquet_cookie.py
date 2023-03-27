# -*- coding: utf-8 -*-
"""
Created on Fri Feb 24 15:19:15 2023

@author: ArdoJ
"""

import os
os.environ['USE_PYGEOS'] = '0'
import pandas as pd
import geopandas as gp
from shapely.geometry import box
import dask_geopandas as dg
import h3pandas
import numpy as np
import threading
import time
import functools
st = time.time()


df=gp.read_file('C:/Users/ArdoJ/Documents/NRC LUM/data/lris-lcdb-v50-land-cover-database-version-50-mainland-new-zealand-FGDB (1)/lcdb-v50-land-cover-database-version-50-mainland-new-zealand.gdb')
out='lcdb/'


#Generating H3 COOKIE CUTTER
df=df.explode(index_parts=False)
df=df.to_crs(crs=4326)
xmin,ymin,xmax,ymax =  df.total_bounds
boundbox=box(*df.total_bounds)
boundbox = gp.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[boundbox])  
cookiecut= boundbox.h3.polyfill_resample(5).drop(columns=['index'])
cookiecut= cookiecut.to_crs(2193)
cookiecut=cookiecut.reset_index().drop(columns=['h3_polyfill'])

#CREATING DATAFRAME OF SMALL AND LARGE
df = df.to_crs(2193)
df['is_large']=df['geometry'].area/10000>5000
df.groupby('is_large')
dfsmall = df[df['is_large'] ==False]
dflarge = df[df['is_large'] ==True]

del df

#COOKIE CUTTING LARGE DATA
dflarge = dflarge.overlay(cookiecut, how='identity')

df= pd.concat([dflarge, dfsmall])
df= df.drop(columns=['is_large'])
df=df.explode(index_parts=False)

del dfsmall
del dflarge
df=df.to_crs(crs=4326)
ddf=dg.from_geopandas(df, npartitions=150)
ddf = ddf.spatial_shuffle()
ddf.to_parquet(out)


et = time.time()
elapsed_time = et - st
print('Cookie cutting done! Time taken:', elapsed_time/60 ,'min')    
