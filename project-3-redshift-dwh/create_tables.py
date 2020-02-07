import configparser
import psycopg2
import boto3
import json
import pandas as pd
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    A function that that drops all the tables specified in the sql_queries module
    
    Keyword arguments:
    cur -- a cursor to allow running sql commands through the connection
    conn -- a connection to the Postgres database
    """
    print("Dropping tables:")
    i = 1
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print("Dropped table number "+str(i))
        i += 1


def create_tables(cur, conn):
    """
    A function that creates all the tables specified in the sql_queries module
    
    Keyword arguments:
    cur -- a cursor to allow running sql commands through the connection
    conn -- a connection to the Postgres database
    """
    print("Creating tables:")
    i = 1
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print("Created table number "+str(i))
        i += 1


def main():
    """Loads the configuration file and connects to the redshift cluster
     Subsequently, it drops all the tables in the cluster and recreates them
    """
    # load the configuration file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # connect to redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
        
    # drop the table if they exist and then create them
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    conn.close()

if __name__ == "__main__":
    main()