import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, count_table_queries
from create_cluster import get_cluster_info


def load_staging_tables(cur, conn):
    """Run queries to copy data to staging tables.
    
    Args:
        cur: Cursor of the connection.
        conn: Connection to the database.
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Run queries to insert data into analytic tables.
    
    Args:
        cur: Cursor of the connection.
        conn: Connection to the database.
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def count_tables(cur, conn):
    """Run queries to count data rows from loaded tables.
    
    Args:
        cur: Cursor of the connection.
        conn: Connection to the database.
    """
    
    print('Table counts:')
    for query in count_table_queries:
        cur.execute(query)
        print(cur.fetchall()[0][0])

def main():
    """Load data to staging and then analytic tables and check if data has been loaded."""
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    host = get_cluster_info(config)['Endpoint']['Address']

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host, *config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    count_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()