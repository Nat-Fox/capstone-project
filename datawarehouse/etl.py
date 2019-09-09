import configparser
import psycopg2
from sql_queries import copy_table_queries, create_table_queries, drop_table_queries, insert_table_queries

def exec_list_of_queries(cur, conn, queries):  
    """
    Execute queries
    
    Args:
        cur: pyscopg.cursor of the connected database to run queries
        conn: redshift conection instance
        queries: sql queries strings to be executed
    Returns: None
    """
    
    for query in queries:
        cur.execute(query)
        conn.commit()    
        
def drop_staging_tables(cur, conn):
    """
    Drop tables
    
    Args:
        cur: pyscopg.cursor of the connected database to run queries
        conn: redshift conection instance
    Returns: None
    """

    exec_list_of_queries(cur, conn, drop_table_queries)

        
def create_staging_tables(cur, conn):
    """
    Create tables from S3
    
    Args:
        cur: pyscopg.cursor of the connected database to run queries
        conn: redshift conection instance
    Returns: None
    """
    exec_list_of_queries(cur, conn, create_table_queries)
        
    
def load_staging_tables(cur, conn):
    """
    Load data from s3 into redshift tables
    
    Args:
        cur: pyscopg.cursor of the connected database to run queries
        conn: redshift conection instance
    Returns: None
    """
    exec_list_of_queries(cur, conn, copy_table_queries)


def insert_tables(cur, conn):
    """
    Transform data from staging tables, and insert into fact and dimension tables
    
    Args:
        cur: pyscopg.cursor of the connected database to run queries
        conn: redshift conection instance
    Returns: None
    """
    
    for query in insert_table_queries:        
        cur.execute(query)
        conn.commit()
    
        

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    try:    
        connection_string = "host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values())
        
        conn = psycopg2.connect(connection_string)
        cur = conn.cursor()        
        
        print('=> Droping staging tables ...')
        drop_staging_tables(cur, conn)
        
        print('=> Creating staging tables ...')
        create_staging_tables(cur, conn)
        
        print('=> Loading data into staging tables ...')
        load_staging_tables(cur, conn)
        
        print('=> Insert data')
        insert_tables(cur, conn)
        
    except(Exception, psycopg2.Error) as error:
        print(f"=> Database error: {error}")
    finally:    
        print("=> Closing database connection ...")
        conn.close()


if __name__ == "__main__":
    main()