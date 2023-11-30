#import sys
#sys.path.insert(0, "/usr/local/airflow/.local/lib/python3.7/site-packages")
# Import Library to access databases from MongoDB (Middleware and VAS)
#import pymongo
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
from datetime import timedelta, datetime
import shutil
import sys



start = datetime(2023,11,29,8,0,0,0)
stop = datetime(2023,11,30,7,59,59,999)

def extract_from_vas():
    # vas credentials
    host = "192.168.0.35"
    port = 11001
    user_name = "vasuser"
    pass_word = "p@$$w0rd@1"
    db_name = "vas"
    client = MongoClient(f'mongodb://{user_name}:{urllib.parse.quote_plus(pass_word)}@{host}:{port}/{db_name}', compressors="snappy")
    db = client['vas']

    print('extracting data from vas for ' + str(stop.strftime('%Y-%m-%d %H:%M:%S')))
    result = db.vas_transaction.find({"updated_at": {"$gt": start,
            "$lte": stop}})
    result_df =  pd.DataFrame(list(result))
    result_df = result_df.astype(str)
    result_df['vas_source'] = 'yes'
    #print(result_df)
    df = result_df
    print(' The length of first diff data is ' + str(len(result_df)))

    # Reloading to make sure that the complete data is ingested from the given time frame
    old_count = 0
    count_df = len(df)

    while count_df != old_count:
        old_count = len(df)
        result = db.vas_transaction.find({"updated_at": {"$gt": start,
            "$lte": stop}})
        result_df =  pd.DataFrame(list(result))
        result_df = result_df.astype(str)
        result_df['vas_source'] = 'yes'
        df = result_df
        count_df = len(df)
        print("data count is " + str(count_df))
        print("Checking for data completion, reloading from VAS")
        
    print("data count is " + str(count_df))
    print("Data complete")
    # prepare to save locally
    df.columns = map(str.lower, df.columns)
    print('The number of row is ' + str(len(df)))

    if not os.path.exists('C:/daniel/dataruns'):
        os.makedirs('C:/daniel/dataruns')
        df.to_csv('C:/daniel/dataruns/' + 'new.csv')
        print('Extraction done ' + str(start.strftime('%Y-%m-%d %H:%M:%S')))
    else:
        df.to_csv('C:/daniel/dataruns/' + 'new.csv')
        print('Extraction done ' + str(start.strftime('%Y-%m-%d %H:%M:%S')))


def load_vas_to_dwh():
    conn = psy.connect(dbname='data_warehouse', user='itex_user', password='ITEX2022', host='192.168.0.242', port='5432')
    engine = create_engine('postgresql://itex_user:ITEX2022@192.168.0.242:5432/data_warehouse')
    conn.autocommit = True
    folder = os.listdir('C:/daniel/dataruns/')
    for i in folder:
        if len(folder) > 0:
            print('folder contains a file')
            vas_df = pd.read_csv('C:/daniel/dataruns/' + str(i)) 
            #vas_df.reset_index(drop=True)
            print('The number of rows in data is ' + str(len(vas_df)))
            print('loading vas transactions to warehouse...')
            try:
                cursor = conn.cursor()
                print('creating vas_transactions table...')
                try:
                    del vas_df['Unnamed: 0']
                except:
                    pass 
           
                vas_df.to_sql('vas_transactions', engine, schema='galaxy_schema', if_exists='append', index=False, dtype={col_name: sqlalchemy.types.Text() for col_name in vas_df})
                print('Vas transaction loaded  to data warehouse ')

                # alter table to create vas_id, this will be use to create schema
                cursor = conn.cursor()
                cursor.execute("ALTER TABLE galaxy_schema.vas_transactions ADD COLUMN IF NOT EXISTS vas_id SERIAL PRIMARY KEY;")
                # Commit your changes in the database
                conn.commit()
                conn.close()
            except:
                engine = create_engine('postgresql://itex_user:ITEX2022@192.168.0.242:5432/data_warehouse')
                print("Direct loading failed due to extra column, creating additional column")
                sql = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS \
                    WHERE table_name = 'vas_transactions'"
                tb_df = pd.read_sql(sql, con=engine)
                tb_ls = tb_df['column_name'].values.tolist()
                vas_ls= vas_df.columns.tolist()
                print('getting the extra columns')
                col_dif = set(vas_ls) - set(tb_ls)
                print(col_dif)
                col_dif2 = list(col_dif) 
                print(col_dif2)
                #Creating a cursor object using the cursor() method
                cursor = conn.cursor()
                print('creating columns in table') 
                for l in col_dif2:
                    if l == 'to':
                        del vas_df[str(l)]
                    else:
                        print(l)
                        cursor.execute('ALTER TABLE galaxy_schema.%s ADD COLUMN IF NOT EXISTS %s text' % ('vas_transactions', str(l)))        
                    # Commit your changes in the database
                    conn.commit()
                
                print('retrying data load to table...')
                try:
                    del vas_df['Unnamed: 0']
                except:
                    pass 
                vas_df.to_sql('vas_transactions', engine, schema='galaxy_schema', if_exists='append', index=False,  dtype={col_name: sqlalchemy.types.Text() for col_name in vas_df})

                # alter table to create vas_id, this will be use to create schema
                cursor = conn.cursor()
                cursor.execute("ALTER TABLE galaxy_schema.vas_transactions ADD COLUMN IF NOT EXISTS vas_id SERIAL PRIMARY KEY;")
                # Commit your changes in the database
                conn.commit()
                conn.close()

        else:   
            print('folder is empty')



def extract_from_mdw():
    # middleware credentials
    host = "197.253.19.75"
    port = 22002
    user_name = "dataeng"
    pass_word = "4488qwe"
    db_name = "admin"
    client = MongoClient(f'mongodb://{user_name}:{urllib.parse.quote_plus(pass_word)}@{host}:{port}/{db_name}')
    db = client['eftEngine']
    
    print('Extracting data from middleware from ' + str(start.strftime('%Y-%m-%d %H:%M:%S')) + ' to ' + str(stop.strftime('%Y-%m-%d %H:%M:%S')))
    #result1 = db.journals_22_04_06.find({"transactionTime": {"$gt": start,
    #        "$lte": stop}})
    #result_df1 =  pd.DataFrame(list(result1))

    result2 = db.journals_23_10_12.find({"transactionTime": {"$gt": start,
            "$lte": stop}})
    result_df2 =  pd.DataFrame(list(result2))
    
    #df = pd.concat([result_df1, result_df2], ignore_index=True)
    df = result_df2    

    try:
        shutil.rmtree('/usr/local/airflow/transactions/mdw')
        print('old file removed')
    except:
        pass

    #print('The number of row is ' + str(len(df)))

    # Reloading to make sure that the complete data is ingested from the given time frame
    old_count = 0
    count_df = len(df)

    while count_df != old_count:
        old_count = len(df)
        print('Extracting data from middleware from ' + str(start.strftime('%Y-%m-%d %H:%M:%S')) + ' to ' + str(stop.strftime('%Y-%m-%d %H:%M:%S')))

        result2 = db.journals_23_10_12.find({"transactionTime": {"$gt": start,
            "$lte": stop}})
        df =  pd.DataFrame(list(result2))
        count_df = len(df)
        print("data count is " + str(count_df))
        print("Confirming that data is complete")
        
    print("data count is " + str(count_df))
    print("Data complete")
    # prepare to save locally
    df.columns = map(str.lower, df.columns)
    print('The number of row is ' + str(len(df)))
    df['mdw_source'] = 'yes'


    if not os.path.exists('C:/daniel/mdwruns'):
        os.makedirs('C:/daniel/mdwruns')
        df.to_csv('C:/daniel/mdwruns/' + str(start.strftime('%Y-%m-%d %H:%M:%S')) + '.csv')
        print('Extraction done ' + str(start.strftime('%Y-%m-%d %H:%M:%S')))
    else:
        df.to_csv('C:/daniel/mdwruns/' + 'mdw.csv')
        print('Extraction done ' + str(start.strftime('%Y-%m-%d %H:%M:%S')))


def load_mdw_to_dwh():
    conn = psy.connect(dbname='data_warehouse', user='itex_user', password='ITEX2022', host='192.168.0.242', port='5432')
    engine = create_engine('postgresql://itex_user:ITEX2022@192.168.0.242:5432/data_warehouse')
    conn.autocommit = True
    folder = os.listdir('C:/daniel/mdwruns/')
    for i in folder:
        if len(folder) > 0:
            print('folder contains a file')
            mdw_df = pd.read_csv('C:/daniel/mdwruns/' + str(i))
            mdw_df.reset_index(drop=True)
            #mdw_df = mdw_df.drop('Unnamed: 0')
            print('The number of rows in data is ' + str(len(mdw_df)))
            print('loading mdw transactions to warehouse...')
            try:
                del mdw_df['Unnamed: 0']
            except:
                  pass 
            try:
                mdw_df.to_sql('mdw_transactions_2023_july', engine,  schema='galaxy_schema', if_exists='append', index=False,  dtype={col_name: sqlalchemy.types.Text() for col_name in mdw_df})
                print('mdw transaction loaded  to data warehouse ')

                # alter table to create mdw_id, this will be use to create schema
                cursor = conn.cursor()
                cursor.execute("ALTER TABLE galaxy_schema.mdw_transactions_2023_july ADD COLUMN IF NOT EXISTS mdw_id SERIAL PRIMARY KEY;")
                # Commit your changes in the database
                conn.commit()
                conn.close()
            except:
                engine = create_engine('postgresql://itex_user:ITEX2022@192.168.0.242:5432/data_warehouse')
                print("Direct loading failed due to extra column, creating additional column")
                sql2 = "SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS \
                    WHERE table_name = 'mdw_transactions_2023_july'"
                
                tb_df2 = pd.read_sql(sql2, con=engine)
                tb_ls2 = tb_df2['column_name'].values.tolist()
                mdw_ls= mdw_df.columns.tolist()
                print('getting the extra columns')
                col_dif2 = set(mdw_ls) - set(tb_ls2)
                print(col_dif2)
                col_dif2 = list(col_dif2) 
                print(col_dif2)
                #Creating a cursor object using the cursor() method
                cursor = conn.cursor()
                print('creating columns in table') 
                for l in col_dif2:
                    cursor.execute('ALTER TABLE galaxy_schema.%s ADD COLUMN IF NOT EXISTS %s text' % ('mdw_transactions_2023_july', str(l)))        
                    # Commit your changes in the database
                    conn.commit()
                
                print('retrying data load to table...')
                try:
                    del mdw_df['Unnamed: 0']
                except:
                    pass 
                mdw_df.to_sql('mdw_transactions_2023_july', engine,  schema='galaxy_schema', if_exists='append', index=False, dtype={col_name: sqlalchemy.types.Text() for col_name in mdw_df})

                # alter table to create mdw_id, this will be use to create schema
                cursor = conn.cursor()
                cursor.execute("ALTER TABLE galaxy_schema.mdw_transactions_2023_july ADD COLUMN IF NOT EXISTS mdw_id SERIAL PRIMARY KEY;")
                # Commit your changes in the database
                print('loading to data warehouse completed')
                conn.commit()
                conn.close()
        else:
            print('folder is empty')

def extract_load_agentdata():

    engine_source = create_engine('postgresql://admin:tams@192.168.0.134:5432/tams')
    print('extracting agent data from tams...')
    tams_agent_data_df = pd.read_sql('SELECT * FROM agentdata', con=engine_source)

    # Drop existing profile db, and load current one
    conn = psy.connect(database="data_warehouse", user='itex_user', password='ITEX2022', host='192.168.0.242', port= '5432')
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute('''DROP TABLE IF EXISTS galaxy_schema.agentdata_dim ''')
    print("Table dropped !")
    conn.commit()
    conn.close()
    
    #load agent data to warehouse
    engine1 = create_engine('postgresql://itex_user:ITEX2022@192.168.0.242:5432/data_warehouse')
    tams_agent_data_df.to_sql('agentdata_dim', con=engine1, schema='galaxy_schema', if_exists='replace', index=False, dtype={col_name: sqlalchemy.types.VARCHAR for col_name in tams_agent_data_df})
    print('agentdata dim loaded  to data warehouse ')  
    

def extract_load_users():
    engine_source = create_engine('postgresql://admin:tams@192.168.0.134:5432/tams')
    print('extracting users data from tams...')
    tams_users_df = pd.read_sql('SELECT * FROM users', con=engine_source)

    # Drop existing profile db, and load current one
    conn = psy.connect(database="data_warehouse", user='itex_user', password='ITEX2022', host='192.168.0.242', port= '5432')
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute('''DROP TABLE IF EXISTS galaxy_schema.users_dim ''')
    print("Table dropped !")
    conn.commit()
    conn.close()
    
    #load agent data to warehouse
    engine1 = create_engine('postgresql://itex_user:ITEX2022@192.168.0.242:5432/data_warehouse')
    tams_users_df.to_sql('users_dim', con=engine1, schema='galaxy_schema', if_exists='replace', index=False, dtype={col_name: sqlalchemy.types.VARCHAR for col_name in tams_users_df})
    print('users dim loaded to data warehouse')  

    # Configuring a primary key for modelling
    conn = psy.connect(database="data_warehouse", user='itex_user', password='ITEX2022', host='192.168.0.242', port= '5432')
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute('''ALTER TABLE galaxy_schema.users_dim ADD CONSTRAINT pk_usr_irn PRIMARY KEY (usr_irn) ''')
    print("Primary key added!")
    conn.commit()
    conn.close()


def clean_directory():
    file_path = 'C:/daniel/mdwruns/mdw.csv'  # Replace with the actual file path

    try:
        os.remove(file_path)
        print(f"File '{file_path}' deleted successfully.")
    except FileNotFoundError:
        print(f"File '{file_path}' not found.")
    except PermissionError:
        print(f"Permission denied. Unable to delete file '{file_path}'.")
    except Exception as e:
        print(f"An error occurred while deleting the file: {e}")


    file_path2 = 'C:/daniel/dataruns/new.csv'  # Replace with the actual file path

    try:
        os.remove(file_path2)
        print(f"File '{file_path2}' deleted successfully.")
    except FileNotFoundError:
        print(f"File '{file_path2}' not found.")
    except PermissionError:
        print(f"Permission denied. Unable to delete file '{file_path2}'.")
    except Exception as e:
        print(f"An error occurred while deleting the file: {e}")


extract_from_vas()
load_vas_to_dwh()
extract_from_mdw()
load_mdw_to_dwh()
extract_load_agentdata()
extract_load_users()
clean_directory()

print('DWH Pipeline run success')
