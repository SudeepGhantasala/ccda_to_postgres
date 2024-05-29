#imports
import psycopg2, sys, json, boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine

def get_creds(dbname):
    creds = {"username":"",
             "password":"",
             "engine":"postgres",
             "host":"",
             "port":5432}
    return creds

def connect(dbname):

    conn = None
    db_params = get_creds(dbname)

    try:
        conn = psycopg2.connect(**db_params)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    
    return conn

def sql_alc_connect(dbname):
    db_params = get_creds(dbname)
    return create_engine(f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:5432/{db_params['dbname']}")