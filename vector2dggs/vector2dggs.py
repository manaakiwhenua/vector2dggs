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
from shapely.geometry import box, Polygon, MultiPolygon, GeometryCollection
import dask_geopandas as dg
import h3pandas
import multiprocessing as mp
from multiprocessing.dummy import Pool
import tempfile
import threading
import time
import dask.dataframe as dd
from tqdm.dask import TqdmCallback
from tqdm import tqdm

st = time.time()

import warnings
warnings.filterwarnings("ignore") #This is to filter out the polyfill warnings when rows failed to get indexed at a resolution


#######################################################################################################################################################
def vector2dggs(input_file: Union[Path, str],
           output_file: Union[Path, str],
           resolution: int,
           partitions: int,
           cut_threshold: int,
           threads: int,
           **kwargs
           )-> Path:

    tst= time.time()
    st = time.time()  
    out=output_file
    H3res=resolution                
    npartitions=partitions
    cut=cut_threshold
    corecount=threads

    h3_r=str(H3res).zfill(2)
    
    def katana(geometry, threshold, count=0):
        """Split a Polygon into two parts across it's shortest dimension, 
        code written by Joshua Arnott 2016"""
        bounds = geometry.bounds
        width = bounds[2] - bounds[0]
        height = bounds[3] - bounds[1]
        if max(width, height) <= threshold or count == 250:
            # either the polygon is smaller than the threshold, or the maximum
            # number of recursions has been reached
            return [geometry]
        if height >= width:
            # split left to right
            a = box(bounds[0], bounds[1], bounds[2], bounds[1]+height/2)
            b = box(bounds[0], bounds[1]+height/2, bounds[2], bounds[3])
        else:
            # split top to bottom
            a = box(bounds[0], bounds[1], bounds[0]+width/2, bounds[3])
            b = box(bounds[0]+width/2, bounds[1], bounds[2], bounds[3])
        result = []
        for d in (a, b,):
            c = geometry.intersection(d)
            if not isinstance(c, GeometryCollection):
                c = [c]
            for e in c:
                if isinstance(e, (Polygon, MultiPolygon)):
                    result.extend(katana(e, threshold, count+1))
        if count > 0:
            return result
        # convert multipart into singlepart
        final_result = []
        for g in result:
            final_result.append(g)
        return final_result
    
    #Output directory created
    os.mkdir(out) # Will throw an error if directory already exists, as designed.
    name= os.path.basename(os.path.normpath(out))
    Out= str(out+'/'+name+'_'+h3_r)
    Fileout= str(out+'/'+name+'_'+'_fid.gpkg')

    #Generating fid gpkg file and fid GeoDataFrame
    print('Generating unique fid')
    st=time.time()
    gdf=gp.read_file(input_file) 
    columns= list(gdf)
    columns.remove('geometry')
    gdf['fid']=gdf.groupby(columns, dropna=False).ngroup()
    df=gdf[['fid','geometry']].sort_values(by=['fid'])
    gdf.set_index('fid').sort_index().to_file(Fileout, driver='GPKG')

    et=time.time()
    elapsed_time = et - st
    print('Generating unique fid complete! Time taken:', elapsed_time/60 ,'mins')

    df=df.explode(index_parts=False)
    cols = df.columns
    df=df
    df=df.to_crs(2193)
    
    print('Watch out for ninjas!(Cutting polygons)')
    with tqdm(total=df.shape[0]) as pbar: 
        for index, row in df.iterrows():
            geometry=katana(row['geometry'],cut)
            gc=GeometryCollection(geometry)
            df.loc[index, 'geometry']=gc
            pbar.update(1)
    
    print('Preparing for spatial partitioning... patience please...')
    df=df.explode() #Explode from GeomCollection
    df=df.explode().reset_index() #Explode multipoly to polygons
    df=df[cols]
    temp_dir = tempfile.TemporaryDirectory().name
    df=df.to_crs(4326)
    ddf=dg.from_geopandas(df, npartitions=npartitions)
    ddf = ddf.spatial_shuffle(by="hilbert", npartitions=npartitions)
    
    LOGGER.info(
        "Spatial partitioning %s with Hilbert curve method with partitions: %d",
        name,
        partitions,
    )
    
    with TqdmCallback():
         ddf.to_parquet(temp_dir)

    #Currently Cuts the polygons above 5000ha

    files = os.listdir(temp_dir+'/')
    file_list = files
    st = time.time()
    
    #Polyfilling function defined here
    def polyfill(filename):
        'converts a filename to a pandas dataframe'
        df=gp.read_parquet(temp_dir+'/'+filename)
        df=df[df.geometry.type == 'Polygon']
        h3geo= df.h3.polyfill_resample(H3res, return_geometry=False)
        h3geo= pd.DataFrame(h3geo).drop(columns=['hilbert_distance','geometry'])
        h3geo.to_parquet(Out+'_'+str(filename), compression='ZSTD') 

    #Multithreaded polyfilling
    def poly_stage():
        LOGGER.info(
        "Final stage- H3 Indexing by polyfill with H3 resoltion: %d",
        resolution,
        )
        #TO DO INSERT TQDM PROGRESS BAR HERE
        with Pool(processes=corecount) as pool:
                # have your pool map the file names to dataframes
                #pool.map(polyfill, file_list)
                list(tqdm(pool.imap(polyfill, file_list),total=npartitions))
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
@click.option('-c', "--cut_threshold", required=True, default=5000, type=int, help="Cutting up large polygons into target length", nargs=1)
@click.option('-t', "--threads", required=False, default=7, type=int, help="Amount of threads used for operation", nargs=1)

def h3(input_file, output_file, resolution, partitions, cut_threshold, threads, **kwargs):
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
    
    vector2dggs(input_file, output_file, int(resolution), int(partitions), cut_threshold, threads, **kwargs)

if __name__ == '__main__':
    h3()

