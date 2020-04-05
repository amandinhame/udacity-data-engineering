import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from create_cluster import get_cluster_info


def drop_tables(cur, conn):
    """Run queries that drop tables to restart the process.
    
    Args:
        cur: Cursor of the connection.
        conn: Connection to the database.
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Run queries to create the staging and analytic tables.
    
    Args:
        cur: Cursor of the connection.
        conn: Connection to the database.
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Creates/Reset staging and analytic tables."""
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    host = get_cluster_info(config)['Endpoint']['Address']

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(host, *config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()