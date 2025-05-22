from psycopg2 import sql
from psycopg2.extensions import connection

from sql_queries import cf_standardize_cities_query, ndt_standardize_cities_query, cf_delete_abnormal_servers_query, ndt_delete_abnormal_servers_query


def ndt_post_processing(conn: connection) -> None:
    try:
        with conn.cursor() as cur:
            cur.execute(ndt_delete_abnormal_servers_query)
            cur.execute(ndt_standardize_cities_query)
        conn.commit()
        print(f'Successfully finished post-processing of NDT')
    except Exception:
        conn.rollback()
        raise

def cf_post_processing(conn: connection, table_name: str) -> None:
    try:
        with conn.cursor() as cur:
            delete_abnormal_servers_query = cf_delete_abnormal_servers_query.format(sql.Identifier(f'cf_{table_name}'))
            standardize_cities_query = cf_standardize_cities_query.format(sql.Identifier(f'cf_{table_name}'))
            cur.execute(delete_abnormal_servers_query)
            cur.execute(standardize_cities_query)
        conn.commit()
        print(f'Successfully finished post-processing of Cloudflare {table_name}')
    except Exception as e:
        conn.rollback()
        raise