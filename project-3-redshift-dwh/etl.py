import configparser
import psycopg2
import pandas as pd
import boto3
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    A function that loads the staging tables from JSON files
    
    Keyword arguments:
    cur -- a cursor to allow running sql commands through the connection
    conn -- a connection to the Postgres database
    """
    print("Loading staging tables:")
    i = 1
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print("Loaded staging table number "+str(i))
        i += 1


def insert_tables(cur, conn):
    """
    A function that loads the analytics tables from the staging tables
    
    Keyword arguments:
    cur -- a cursor to allow running sql commands through the connection
    conn -- a connection to the Postgres database
    """
    print("Inserting into star tables:")
    i = 1
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print("Inserted into table number "+str(i))
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
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.commit()

    conn.close()


if __name__ == "__main__":
    main()