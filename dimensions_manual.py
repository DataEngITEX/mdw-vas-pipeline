
# add library path
import sys
sys.path.insert(0, "/usr/local/airflow/.local/lib/python3.7/site-packages")
# Import Library to access databases from MongoDB (Middleware and VAS)
import pymongo
import urllib.parse
from pymongo import MongoClient
# import libraries (to access TAMS)
import psycopg2 as psy
import sqlalchemy
from sqlalchemy import create_engine
# import pandas for data transaformation
import pandas as pd
# import utility libraries
import numpy as np
import datetime
import os
from datetime import timedelta
import shutil
import sys
#sys.path.insert(0, "/usr/local/airflow/.local/bin")





''' # FUNCTION TO EXTRACT DATA FROM TAMS DB
'''
def extract_from_tams():
    start = datetime.datetime.now()
    # TAMS CREDENTIALS
    engine_source = create_engine('postgresql://admin:tams@192.168.0.134:5432/tams')
    print('extracting data from tams...')
    tams_merchant_df = pd.read_sql('SELECT * FROM merchant', con=engine_source) 

    
    # save first part of profile
    if not os.path.exists('C:/daniel/profile'):
        os.makedirs('C:/daniel/profile')
        tams_merchant_df.to_csv('C:/daniel/profile/' + 'profile.csv')
        print('Extraction done for profile 1 ' + str(start.strftime('%Y-%m-%d %H:%M:%S')))
    else:
        tams_merchant_df.to_csv('C:/daniel/profile/' + 'profile.csv')
        print('Extraction done for profile 1 ' + str(start.strftime('%Y-%m-%d %H:%M:%S')))



def profile_dim():
    start = datetime.datetime.now()
    # TAMS CREDENTIALS
    #conn_string = 'postgres://itex_user:ITEX2022@192.168.0.242/data_warehouse'
    engine1 = create_engine('postgresql://itex_user:ITEX2022@192.168.0.242:5432/data_warehouse')
    engine_source = create_engine('postgresql://admin:tams@192.168.0.134:5432/tams')
    print('extracting data from tams...')
    tams_terminals_df = pd.read_sql('SELECT * FROM terminals', con=engine_source)
    tams_terminals_df['mht_irn'] = tams_terminals_df['trm_mht_irn']
    tams_terminals_df['tid'] = tams_terminals_df['trm_termid']
    # vas credentials
    host = "192.168.0.35"
    port = 11001
    user_name = "vasuser"
    pass_word = "p@$$w0rd@1"
    db_name = "vas"
    client = MongoClient(f'mongodb://{user_name}:{urllib.parse.quote_plus(pass_word)}@{host}:{port}/{db_name}')
    db = client['vas']
    # extract collections from vas
    print('extracting data from vas...')
    result = db.nqr_ptsp_merchants.find()
    nqr_ptsp_merchant_df =  pd.DataFrame(list(result))

    folder11 = os.listdir('C:/daniel/profile/')
    for i in folder11:
        if len(folder11) > 0:
            print('folder contain a file')
            tams_merchant_df = pd.read_csv('C:/daniel/profile/' + str(i)) 
            tams_merchant_df.reset_index(drop=True)

    print('joining df3 and df4...')
    tams_merchant_df = tams_merchant_df.merge(tams_terminals_df, on='mht_irn', how='outer')
    print('joining df5 and df6...')
    tams_nqr_ptsp_merchant_df = tams_merchant_df.merge(nqr_ptsp_merchant_df, on='tid', how='outer')
    
    print('The number of rows in data is ' + str(len(tams_nqr_ptsp_merchant_df)))
     
    # Drop existing profile db, and load current one
    conn = psy.connect(database="data_warehouse", user='itex_user', password='ITEX2022', host='192.168.0.242', port= '5432')
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute('''DROP TABLE IF EXISTS galaxy_schema.profiles_dim ''')
    print("Table dropped !")
    conn.commit()
    conn.close()

    # loading all profile data to warehouse crash airflow, split data and load in chunks
    for start in range(0, len(tams_nqr_ptsp_merchant_df), 10000):
        small_df = tams_nqr_ptsp_merchant_df[start:start+10000]
        cols = list(tams_nqr_ptsp_merchant_df.columns.values)
        small_df = pd.DataFrame(small_df, columns=cols )
        print('changing column types to strings...')
        #print(small_df)
        
        small_df = small_df.astype(str)
        print('loading dataframe ' + str(start) + ' to data warehouse')
        small_df.to_sql('profiles_dim', con=engine1, schema='galaxy_schema', if_exists='append', index=False, dtype={col_name: sqlalchemy.types.VARCHAR for col_name in tams_nqr_ptsp_merchant_df})
    print('profile dim loaded  to data warehouse ')    


''' # EXTRACT LOCATION DIMENSION FROM PROFILE DIMENSION AND LOAD TO DATA WAREHOUSE
'''
def location_dim():
    # TAMS CREDENTIALS
    engine_dwh = create_engine('postgresql://itex_user:ITEX2022@192.168.0.242:5432/data_warehouse')
    engine_tams = create_engine('postgresql://admin:tams@192.168.0.134:5432/tams')
    
    #extract tables
    profiles_dim_df = pd.read_sql('SELECT mht_irn, mht_name, mht_addr, mht_code, mht_addrcity, mht_addrstate FROM galaxy_schema.profiles_dim', engine_dwh) 
    print(profiles_dim_df)
    tams_states_df = pd.read_sql('SELECT * FROM states;', engine_tams)
    tams_region_df = pd.read_sql('SELECT reg_irn, reg_name, reg_city FROM regions;', engine_tams)
    #tams_region_df['reg_irn'] = tams_region_df['mht_reg_irn']
    tams_cities_df = pd.read_sql('SELECT * FROM cities;', engine_tams)

    # join all dataframes
    location_df = pd.DataFrame()
    profiles_dim_df['stn_code'] = profiles_dim_df['mht_addrstate']
    location_df = profiles_dim_df.merge(tams_states_df, on = 'stn_code', how = 'left')
    location_df['ctn_stn_code'] = location_df['stn_code']
    print(location_df)

    #rename stn and code to state
    list_of_cols = {'stn_name' : 'state',
                    'stn_code' : 'state_code'}
    location_df.rename(columns=list_of_cols, inplace=True)
    
    # Drop existing location table, and load current one
    conn = psy.connect(database="data_warehouse", user='itex_user', password='ITEX2022', host='192.168.0.242', port= '5432')
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute('''DROP TABLE IF EXISTS galaxy_schema.location_dim ''')
    print("Table dropped !")
    conn.commit()
    conn.close()

    # loading all location data to warehouse crash airflow, split data and load in chunks
    for start in range(0, len(location_df), 4000):
        small_df = location_df[start:start+4000]
        cols = list(location_df.columns.values)
        small_df = pd.DataFrame(small_df, columns=cols )
        print('changing column types to strings...')
        
        small_df = small_df.astype(str)
        print('loading dataframe ' + str(start) + ' to data warehouse')
        small_df.to_sql('location_dim', con=engine_dwh, schema='galaxy_schema', if_exists='append', index=False, dtype={col_name: sqlalchemy.types.VARCHAR for col_name in location_df})
    print('Location dim loaded  to data warehouse')

extract_from_tams()
profile_dim()
location_dim()