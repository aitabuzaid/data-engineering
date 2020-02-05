import configparser
import psycopg2
import pandas as pd
import boto3
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    print("Loading staging tables:")
    i = 1
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print("Loaded staging table number "+str(i))
        i += 1


def insert_tables(cur, conn):
    print("Inserting into star tables:")
    i = 1
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        print("Inserted into table number "+str(i))
        i += 1


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.commit()

    conn.close()


if __name__ == "__main__":
    main()