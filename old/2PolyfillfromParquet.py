# -*- coding: utf-8 -*-
"""
Created on Mon Feb 13 14:37:40 2023

@author: ArdoJ
"""
import os
os.environ['USE_PYGEOS'] = '0'
import pandas as pd
import geopandas as gp
import h3pandas
from multiprocessing.dummy import Pool
import threading
import time
st = time.time()


directory='C:/Users/ArdoJ/Documents/NRC LUM/H3hex/codes/polygon_codes/lcdb2/'
Filename='lcdbindexed_.parquet'
path='h3_11'

os.mkdir(path)
Out= str(path+'/'+Filename)

def togeo(filename):
    'converts a filename to a pandas dataframe'
    df=gp.read_parquet(directory+filename)
    df=df[df.geometry.type == 'Polygon']
    h3geo= df.h3.polyfill_resample(11, return_geometry=False)
    h3geo= pd.DataFrame(h3geo).drop(columns=['hilbert_distance','geometry'])
    print(h3geo)
    h3geo.to_parquet(Out+str(filename), compression='ZSTD') 


def main():

    # get a list of file names
    files = os.listdir(directory)
    file_list = files
    
    with Pool(processes=16) as pool:

            # have your pool map the file names to dataframes
            pool.map(togeo, file_list) 
    
if __name__ == '__main__':
    main()

et = time.time()
elapsed_time = et - st
print('Geopackage created, time taken:', elapsed_time/60 ,'min')    
