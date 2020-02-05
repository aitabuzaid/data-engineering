import configparser
import psycopg2
import boto3
import json
import pandas as pd
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    print("Dropping tables:")
    i = 1
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print("Dropped table number "+str(i))
        i += 1


def create_tables(cur, conn):
    print("Creating tables:")
    i = 1
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print("Created table number "+str(i))
        i += 1


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    #conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_ENDPOINT, DWH_DB, DWH_DB_USER ,DWH_DB_PASSWORD, DWH_PORT))
    
    cur = conn.cursor()
    
    
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    conn.close()

if __name__ == "__main__":
    main()