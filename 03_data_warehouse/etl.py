import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, insert_temp_table_queries, drop_temp_table_queries

def load_staging_tables(cur, conn):
    """Loads data into staging tables"""
    for query in copy_table_queries:
        print(query)
        print("---------------------------------------------------------------------------------------------")
        print("running...")
        cur.execute(query)
        print("finished, waiting commit")
        conn.commit()
        print("commited")
    print("load_staging_tables finished.")


def insert_tables(cur, conn):
    """Inserts data into all tables"""
    for query in insert_table_queries:
        print(query)
        print("--------------------------------------------------------------------------------------------")
        cur.execute(query)
        conn.commit()

def insert_temp_tables(cur, conn):
    """Inserts data into temp tables in order to prevent duplicated rows"""
    for query in insert_temp_table_queries:
        print(query)
        print("--------------------------------------------------------------------------------------------")
        cur.execute(query)
        conn.commit()
        
def drop_temp_tables(cur, conn):
    """Drops all temp tables """
    for query in drop_temp_table_queries:
        cur.execute(query)
        conn.commit()
                
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_temp_tables(cur, conn)
    insert_tables(cur, conn)
    drop_temp_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()