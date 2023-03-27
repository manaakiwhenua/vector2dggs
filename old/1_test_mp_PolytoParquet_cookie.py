import os
os.environ['USE_PYGEOS'] = '0'
import pandas as pd
import geopandas as gp
from shapely.geometry import box
import dask_geopandas as dg
import h3pandas
import multiprocessing as mp
from multiprocessing.dummy import Pool
import tempfile
import threading
import time
st = time.time()

filein=gp.read_file('C:/Users/ArdoJ/Documents/NRC LUM/data/lris-lcdb-v50-land-cover-database-version-50-mainland-new-zealand-FGDB (1)/lcdb-v50-land-cover-database-version-50-mainland-new-zealand.gdb')               
out='lcdb_cut'
H3res= 11                #INPUT DESIRED H3 INDEX
cookiecutH3res=5
npartitions=150

#######################################################################################################################################################

parentres= cookiecutH3res
parentres= max(1, parentres)

h3_r=str(H3res).zfill(2)
parent_r=str(parentres).zfill(2)

allcores= mp.cpu_count()
num_workers=mp.cpu_count()-1

df=filein.explode(index_parts=False)
df=df.to_crs(crs=4326)

temp_dir = tempfile.TemporaryDirectory().name
ddf=dg.from_geopandas(df, npartitions=npartitions)
ddf = ddf.spatial_shuffle()
ddf.to_parquet(temp_dir) 

et = time.time()
elapsed_time = et - st
print('Step 1 (spatial partitioning of vectors) done! Time taken:', elapsed_time/60 ,'min')    
st = time.time()

#Currently Cuts the polygons above 5000ha to H3 hexagons of h3res-8 size

xmin,ymin,xmax,ymax =  df.total_bounds
boundbox=box(*df.total_bounds)
boundbox = gp.GeoDataFrame(index=[0], crs='epsg:4326', geometry=[boundbox])  
cookiecut= boundbox.h3.polyfill_resample(parentres).drop(columns=['index'])
cookiecut= cookiecut.to_crs(2193)
cookiecut=cookiecut.reset_index().drop(columns=['h3_polyfill'])

def cookiecutter(df):
    df=gp.read_parquet(temp_dir+'/'+df)
    df = df.to_crs(2193)
    df['is_large']=df['geometry'].area/10000>5000
    df.groupby('is_large')
    dfsmall = df[df['is_large'] ==False]
    dflarge = df[df['is_large'] ==True]
    del df
    dflarge=dflarge.overlay(cookiecut, how='identity', keep_geom_type=False) 
    df= pd.concat([dflarge, dfsmall])
    df= df.drop(columns=['is_large'])
    df=df.explode(index_parts=False)
    del dfsmall
    del dflarge
    df=df.to_crs(crs=4326)
    print('Cookie cutting completed on partition!')
    return(df)    

files = os.listdir(temp_dir+'/')
file_list = files  
  
#Multithreaded cookie cutting
with Pool(num_workers) as pool:
    # have your pool map the file names to dataframes
    df_list=pool.map(cookiecutter, file_list) 
    df = pd.concat(df_list)
    ddf=dg.from_geopandas(df, npartitions=150)
    ddf = ddf.spatial_shuffle()
    ddf.to_parquet(out)
    print('Step 2 (Cookiecutting) done! Time taken:', elapsed_time/60 ,'min')
             
