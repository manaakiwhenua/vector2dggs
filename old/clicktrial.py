# -*- coding: utf-8 -*-
"""
Created on Mon Mar 27 13:36:56 2023

@author: ArdoJ
"""
import errno
import logging
import os
import multiprocessing
from numbers import Number
import numpy as np
from pathlib import Path
import tempfile
import threading
from typing import Tuple, Union
from urllib.parse import urlparse
import click
import click_log 

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
import dask.dataframe as dd
st = time.time()



#######################################################################################################################################################
def vector2dggs(input_file: Union[Path, str],
           output_file: Union[Path, str],
           resolution: int,
           partitions: int,
           cookiecut: bool,
           cookiecut_resolution: int,
           threads: int,
           **kwargs
           )-> Path:
    tst= time.time()
    st = time.time()
    filein=gp.read_file(input_file)   
    out=output_file
    H3res=resolution                
    npartitions=partitions
    cut=cookiecut     
    cookiecutH3res=cookiecut_resolution
    corecount=threads

    parentres= int(cookiecutH3res)
    parentres= max(1, parentres)

    h3_r=str(H3res).zfill(2)
    parent_r=str(parentres).zfill(2)


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

    #Currently Cuts the polygons above 5000ha

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
        return(df)    

    files = os.listdir(temp_dir+'/')
    file_list = files  
    
    #Multithreaded cookie cutting
    if cut:
        with Pool(processes=corecount) as pool:
                # have your pool map the file names to dataframes
                df_list=pool.map(cookiecutter, file_list) 
                df = pd.concat(df_list)
                temp_dir = tempfile.TemporaryDirectory().name
                ddf=dg.from_geopandas(df, npartitions=npartitions)
                ddf = ddf.spatial_shuffle()
                ddf.to_parquet(temp_dir) 
                print('Step 2 (Cookiecutting) done! Time taken:', elapsed_time/60 ,'min') 

    files = os.listdir(temp_dir+'/')
    file_list = files
    print('Polyfilling stage commencing')
    st = time.time()
    #Polyfills with multithreading
    os.mkdir(out)
    Out= str(out+'/'+out+h3_r)

    def polyfill(filename):
        'converts a filename to a pandas dataframe'
        df=gp.read_parquet(temp_dir+'/'+filename)
        df=df[df.geometry.type == 'Polygon']
        h3geo= df.h3.polyfill_resample(H3res, return_geometry=False)
        h3geo= pd.DataFrame(h3geo).drop(columns=['hilbert_distance','geometry'])
        h3geo.to_parquet(Out+'_'+str(filename), compression='ZSTD') 

    def poly_stage():
        with Pool(processes=corecount) as pool:
                # have your pool map the file names to dataframes
                pool.map(polyfill, file_list) 
    poly_stage()

    et = time.time()
    elapsed_time = et - st
    print('Polyfill done! Time taken:', elapsed_time/60 ,'min') 
    
    et = time.time()
    elapsed_time = et - tst
    print('Total time taken:', elapsed_time/60 ,'min') 

LOGGER = logging.getLogger(__name__)
click_log.basic_config(LOGGER)
MIN_H3, MAX_H3 = 0, 15
DEFAULT_NAME: str = "value"

@click.command(context_settings={"show_default": True})
@click_log.simple_verbosity_option(LOGGER)
@click.argument('input_file', required=True, type=click.Path(), nargs=1)
@click.argument('output_file', required=True, type=click.Path(), nargs=1)
@click.option('-r', "--resolution", required=True, type=click.Choice(list(map(str, range(MIN_H3, MAX_H3 + 1)))), help="H3 resolution to index", nargs=1)
@click.option('-p', "--partitions", required=True, type=str, default=50, help="Geo-partitioning, currently only available in Hilbert method", nargs=1)
@click.option('-cc','--cookiecut', default=False,type=bool, required=False, help='bool, Apply cookie-cutting to the input polygons')
@click.option('-ccr', '--cookiecut_resolution',required=False, type=click.Choice(list(map(str, range(MIN_H3, MAX_H3 + 1)))), default='3', help='H3 resolution to use for cookie-cutting', nargs=1)
@click.option('-t', "--threads", required=False, default=7, type=int, help="Amount of threads used for operation", nargs=1)

def h3(input_file, output_file, resolution, partitions, cookiecut, cookiecut_resolution, threads, **kwargs):
    if not Path(input_file).exists():
        if not urlparse(input_file).scheme:
            LOGGER.warning(
                f"Input raster {input_file} does not exist, and is not recognised as a remote URI"
            )
            raise FileNotFoundError(
                errno.ENOENT, os.strerror(errno.ENOENT), input_file
            )
        input_file = str(input_file)
    else:
        input_file = Path(input_file)
    
    vector2dggs(input_file, output_file, int(resolution), int(partitions), cookiecut, cookiecut_resolution, threads, **kwargs)

if __name__ == '__main__':
    h3()

