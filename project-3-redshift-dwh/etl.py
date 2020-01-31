import configparser
import psycopg2
import pandas as pd
import boto3
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    s3 = boto3.resource('s3',
                   region_name = "us-west-2",
                   aws_access_key_id=KEY,
                   aws_secret_access_key=SECRET)
    s3_log_bucket =  s3.Bucket('udacity-dend')
    #s3_log_bucket.download_file('log_json_path.json', 'log_json_path.json')
    #for obj in s3_log_bucket.objects.filter(Prefix='log_json'):
    #    print(obj.get()['Body'])
    #print('s3://udacity-dend/log_json_path.json'))
    
    load_staging_tables(cur, conn)
    #insert_tables(cur, conn)
    #print(cur.execute("SELECT * FROM staging_songs"))
    conn.commit()

    conn.close()


if __name__ == "__main__":
    main()