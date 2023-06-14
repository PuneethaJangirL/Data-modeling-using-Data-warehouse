import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Dropping staging and final tables if exists
    """
    
    #drop tables
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creating staging and final tables if exists
    """
    
    #create tables
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        - Read DB credential from config file
        - Dropping staging and final tables if exists
        - Creating staging and final tables
    """
    #read config from dwh.cfg
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #connect to database
    conn = psycopg2.connect("".format(*config['CLUSTER'].values()))
    
    print('connected')
    print(conn)
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()