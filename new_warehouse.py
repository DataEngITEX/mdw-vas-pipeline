# Import Library to access databases from MongoDB (Middleware and VAS)
import urllib.parse
from pymongo import MongoClient
# import libraries (to access TAMS)
import psycopg2 as psy
import sqlalchemy
from sqlalchemy import create_engine
# import pandas for data transaformation
import pandas as pd
# import utility libraries
import json
import numpy as np
import datetime
import os
from datetime import timedelta, datetime
import shutil

# Adding the configuration file to boost credential security
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
config_path = '\\'.join([ROOT_DIR, 'credentials.json'])

# read json file
with open(config_path) as config_file:
    config = json.load(config_file)
    config_vas = config['VAS']
    config_mdw = config['Middleware']
    config_dwh = config['DWH']
    

start = datetime(2024,2,12,8,0,0,0)
stop = datetime(2024,2,12,9,59,59,999)

def extract_from_vas():
    # vas credentials
    host = config_vas["HOST"]
    port = config_vas["PORT"]
    user_name = config_vas["USERNAME"]
    pass_word = config_vas["PASSWORD"]
    db_name = config_vas["DB"]

    client = MongoClient(f'mongodb://{user_name}:{urllib.parse.quote_plus(pass_word)}@{host}:{port}/{db_name}', compressors="snappy")
    db = client['vas']

    print('extracting data from vas from ' + str(start.strftime('%Y-%m-%d %H:%M:%S')))

    result = db.vas_transaction.find(
        {"updated_at": 
         {"$gt": start, "$lte": stop}
        },
        {"clientreference": 1, 
         "channel": 1, 
         "paymentmethod": 1
        }
    )
    
    result_df =  pd.DataFrame(list(result))
    result_df = result_df.astype(str)
    
    df = result_df
    print(' The length of first diff data is ' + str(len(result_df)))

    # Reloading for data completion confirmation
    old_count = 0
    count_df = len(df)

    while count_df != old_count:
        old_count = len(df)
        result = db.vas_transaction.find({"updated_at": {"$gt": start,
            "$lte": stop}})
        result_df =  pd.DataFrame(list(result))
        result_df = result_df.astype(str)
        df = result_df
        count_df = len(df)
        print("data count is " + str(count_df))
        print("Checking for data completion, reloading from VAS...")
        
    print("data count is " + str(count_df))
    print("Data complete")
    # prepare to save locally
    df.columns = map(str.lower, df.columns)
    print('The number of row is ' + str(len(df)))

    return df

    """
    if not os.path.exists('C:/daniel/Active projects/mdw_vas/vas_data'):
        os.makedirs('C:/daniel/Active projects/mdw_vas/vas_data')
        df.to_csv('C:/daniel/Active projects/mdw_vas/vas_data/' + 'new.csv')
        print('Extraction done for ' + str(start.strftime('%Y-%m-%d %H:%M:%S')))
    else:
        df.to_csv('C:/daniel/Active projects/mdw_vas/vas_data/' + 'new.csv')
        print('Extraction done for ' + str(start.strftime('%Y-%m-%d %H:%M:%S')))"""

def load_vas_to_warehouse():
    # Connection credentials to warehouse
    host = config_dwh["HOST"]
    dbname = config_dwh["DATABASE"]
    user = config_dwh["USER"]
    password = config_dwh["PASSWORD"]
    port = config_dwh["PORT"]

    conn = psy.connect(dbname=dbname , user=user, password=password, host=host, port=port)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
    conn.autocommit = True
    
    vas_df = extract_from_vas()
    print('loading vas transactions to warehouse...')

    try:
        cursor = conn.cursor()
        print('creating vas_transactions table...')
        try:
            del vas_df['Unnamed: 0']
        except:
            pass 
    
        vas_df.to_sql('vas_transactions', engine, schema='vas_schema', if_exists='append', index=False, dtype={col_name: sqlalchemy.types.Text() for col_name in vas_df})
        print('Vas transaction loaded  to data warehouse ')

        # alter table to create vas_id, this will be use to create schema
        cursor = conn.cursor()
        cursor.execute("ALTER TABLE vas_schema.vas_transactions ADD COLUMN IF NOT EXISTS vas_id SERIAL ;")
        # Commit your changes in the database
        conn.commit()
        conn.close()
    except:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
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
                cursor.execute('ALTER TABLE vas_schema.%s ADD COLUMN IF NOT EXISTS %s text' % ('vas_transactions', str(l)))        
            # Commit your changes in the database
            conn.commit()
        
        print('retrying data load to table...')
        try:
            del vas_df['Unnamed: 0']
        except:
            pass 
        vas_df.to_sql('vas_transactions', engine, schema='vas_schema', if_exists='append', index=False,  dtype={col_name: sqlalchemy.types.Text() for col_name in vas_df})

        # alter table to create vas_id, this will be use to create schema
        cursor = conn.cursor()
        cursor.execute("ALTER TABLE galaxy_schema.vas_transactions ADD COLUMN IF NOT EXISTS vas_id SERIAL;")
        # Commit your changes in the database
        conn.commit()
        conn.close()



def get_vas_responses():
    # vas credentials
    host = config_vas["HOST"]
    port = config_vas["PORT"]
    user_name = config_vas["USERNAME"]
    pass_word = config_vas["PASSWORD"]
    db_name = config_vas["DB"]

    client = MongoClient(f'mongodb://{user_name}:{urllib.parse.quote_plus(pass_word)}@{host}:{port}/{db_name}', compressors="snappy")
    db = client['vas']

    result = db.vas_transaction.find(
        {
            "updated_at": {
                "$gt": start,
                "$lte": stop
            }
        },
        {
            "_id": 0, 
            "response": 1 
        }
    )
    print('Extracting responses...')
    # Convert MongoDB cursor to a list of dictionaries
    result_list = list(result)

    # Convert the list to a DataFrame
    result_df = pd.DataFrame(result_list)
    print('The response volume is ' + str(len(result_df)) + ' rows.')

    # Function to safely load JSON with error handling
    def safe_json_loads(x):
        try:
            return json.loads(x) if isinstance(x, str) else {}
        except json.JSONDecodeError:
            # Log or handle the error as needed
            return {}

    print("Transforming responses data")
    # Replace single quotes with double quotes in the 'response' column
    result_df['response'] = result_df['response'].str.replace("'", '"')

    # Replace hyphens in column names with underscores
    result_df.columns = result_df.columns.str.replace('-', '_')

    # Extract keys from the 'response' column using the safe_json_loads function
    response_keys = result_df['response'].apply(safe_json_loads).apply(pd.Series).columns

    # Expand the 'response' column into separate columns
    result_df = pd.concat([result_df, result_df['response'].apply(safe_json_loads).apply(pd.Series)], axis=1)

    # Drop the original 'response' column
    result_df = result_df.drop('response', axis=1)

    return result_df
    """
    # Save the DataFrame to a CSV file
    result_df.to_csv('C:/daniel/Active projects/mdw_vas/response.csv', index=False)
    print('Processed data downloaded')
    """

def load_responses_to_warehouse():
    
    try:
        # Connection credentials to warehouse
        host = config_dwh["HOST"]
        dbname = config_dwh["DATABASE"]
        user = config_dwh["USER"]
        password = config_dwh["PASSWORD"]
        port = config_dwh["PORT"]

        conn = psy.connect(dbname=dbname , user=user, password=password, host=host, port=port)
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{dbname}')
        conn.autocommit = True

        print('Connected to DWH, loading processed data')
        response_df = get_vas_responses()
        print('Data Loaded')
        try:
            response_df.to_sql('responses', engine, schema='vas_schema', if_exists='append', index=False, dtype={col_name: sqlalchemy.types.Text() for col_name in response_df})
        except Exception as exc:
            print(f'Initial load failed: {exc}')
            with conn.cursor() as cursor:
                cursor.execute("SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = 'responses'")
                tb_columns = [row[0] for row in cursor.fetchall()]

            extra_columns = set(response_df.columns) - set(tb_columns)
            print('Extra columns found:', extra_columns)

            if extra_columns:
                with conn.cursor() as cursor:
                    for column in extra_columns:
                        cursor.execute('ALTER TABLE galaxy_schema.vas_responses ADD COLUMN IF NOT EXISTS "%s" text' % column)
                        print(f'Created missing column: {column}')

            print('Loading Vas responses to data warehouse')
            response_df.to_sql('responses', engine, schema='vas_schema', if_exists='append', index=False, dtype={col_name: sqlalchemy.types.Text() for col_name in response_df})
            print('Vas responses loaded to data warehouse')

    except Exception as e:
        print("An error occurred:", str(e))
    finally:
        if conn is not None:
            conn.close()