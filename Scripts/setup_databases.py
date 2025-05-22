import gc
import pandas as pd

from psycopg2.extensions import connection
from psycopg2.extras import execute_values
from typing import Callable

from sql_queries import get_cf_create_table_sql, ndt_create_table_sql, airports_create_query, countries_with_starlink_measurements_create_query, \
    ndt_best_servers_create_query, cf_best_servers_create_query, cities_create_query, caida_asn_create_table_query


def create_ndt7_table(conn: connection) -> None:
    with conn.cursor() as cur:
        cur.execute(ndt_create_table_sql)
        conn.commit()


def create_cf_tables(conn: connection) -> None:
    with conn.cursor() as cur:
        for table in ['mean', 'median', '90th_percentile']:
            cur.execute(get_cf_create_table_sql(table))
        conn.commit()


def create_airports_table(conn: connection) -> None:
    with conn.cursor() as cur:
        cur.execute(airports_create_query)
        conn.commit()


def create_ndt_servers_table(conn: connection) -> None:
    with conn.cursor() as cur:
        cur.execute(ndt_best_servers_create_query)
        conn.commit()

def create_cf_servers_table(conn: connection) -> None:
    with conn.cursor() as cur:
        cur.execute(cf_best_servers_create_query)
        conn.commit()


def create_cities_table(conn: connection) -> None:
    with conn.cursor() as cur:
        cur.execute(cities_create_query)
        conn.commit()


def create_caida_as_table(conn: connection) -> None:
    with conn.cursor() as cur:
        cur.execute(caida_asn_create_table_query)
        conn.commit()


def create_countries_w_starlink_measurements_table(conn: connection) -> None:
    with conn.cursor() as cur:
        cur.execute(countries_with_starlink_measurements_create_query)
        conn.commit()


def insert_data_from_csv(conn: connection, csv_file_path, insert_query, clean_dataframe: Callable[[pd.DataFrame], None] = None) -> None:
    with conn.cursor() as cur:
        try:
            df = pd.read_csv(csv_file_path, dtype=str, na_values=[""], keep_default_na=False)
            df = df.where(pd.notnull(df), None)
            if clean_dataframe:
                clean_dataframe(df)

            data_tuples = [tuple(x) for x in df.to_records(index=False)]
            execute_values(cur, insert_query, data_tuples)

            conn.commit()
            print(f"Successfully inserted data from {csv_file_path}")

        except Exception as e:
            print(f"Error inserting data from {csv_file_path}: {e}")
            conn.rollback()

        finally:
            del df
            gc.collect()
