import pandas as pd

from pathlib import Path
from psycopg2 import sql
from psycopg2.extensions import connection
from psycopg2.extras import execute_values
from typing import Dict

from post_processing import cf_post_processing, ndt_post_processing
from setup_databases import create_airports_table, create_ndt7_table, insert_data_from_csv, create_cf_tables, create_ndt_servers_table, \
    create_cf_servers_table, create_cities_table, create_caida_as_table, create_countries_w_starlink_measurements_table
from sql_queries import ndt_insert_query, ndt_best_server_insert_query, airport_insert_query, cf_insert_query, cf_best_server_insert_query, cities_insert_query, \
    caida_asn_insert_query, countries_with_starlink_measurements_insert_query


data_dir = Path(__file__).parent / "data"


def populate_ndt_table(conn: connection, csv_name: str = 'ndt7-flat.csv') -> None:
    create_ndt7_table(conn)
    csv_path = data_dir / csv_name
    insert_data_from_csv(conn, csv_path, ndt_insert_query)
    ndt_post_processing(conn)


def populate_cf_tables(conn: connection, csv_names: Dict[str, str] = {'mean': 'cf-flat-mean.csv', 'median': 'cf-flat-median.csv', '90th_percentile': 'cf-flat-90th.csv'}):
    create_cf_tables(conn)
    for table_name, csv_name in csv_names.items():
        csv_path = data_dir / csv_name
        query = cf_insert_query.format(sql.Identifier(f'cf_{table_name}'))
        insert_data_from_csv(conn, csv_path, query)
        cf_post_processing(conn, table_name)
    

def populate_ndt_servers_table(conn: connection, csv_name: str = 'ndt-servers-per-country.csv') -> None:
    def _clean_servers(df: pd.DataFrame) -> None:
        df.dropna(subset=['client_country', 'server_city', 'server_country'], inplace=True)
        df.drop(df[df['client_country'] == df['server_country']].index, inplace=True)

    create_ndt_servers_table(conn)
    csv_path = data_dir / csv_name
    insert_data_from_csv(conn, csv_path, ndt_best_server_insert_query, clean_dataframe=_clean_servers)


def populate_cf_servers_table(conn: connection, csv_name: str = 'cf-servers-per-country.csv') -> None:
    def _clean_codes(df: pd.DataFrame) -> None:
        df.rename(columns={'clientCountry': 'client_country', 'serverPoP': 'server_airport_code'}, inplace=True)
        mask = (
            df['client_country'].str.len().gt(2) |
            df['server_airport_code'].str.len().gt(3)
        )
        df.drop(index=df[mask].index, inplace=True)
    create_cf_servers_table(conn)
    csv_path = data_dir / csv_name
    insert_data_from_csv(conn, csv_path, cf_best_server_insert_query, clean_dataframe=_clean_codes)


def populate_airport_codes_table(conn: connection, csv_name: str = 'airport-codes.csv') -> None:
    def _preprocess_df(df: pd.DataFrame) -> None:
        df.dropna(subset=['iata_code'], inplace=True)
        for col in df.columns:
            if col not in ['iso_country', 'iata_code']:
                df.drop(columns=col, inplace=True)
        df.rename(columns={'iso_country': 'country_code', 'iata_code': 'airport_code'}, inplace=True)

    create_airports_table(conn)
    csv_path = data_dir / csv_name
    insert_data_from_csv(conn, csv_path, airport_insert_query, clean_dataframe=_preprocess_df)


def populate_caida_asn_table(conn: connection, csv_name: str = 'asns_query_results.csv') -> None:
    create_caida_as_table(conn)
    csv_path = data_dir / csv_name
    insert_data_from_csv(conn, csv_path, caida_asn_insert_query)


def populate_countries_w_starlink_measurements_table(conn: connection, csv_name: str = 'countries_w_starlink_measurements.csv') -> None:
    create_countries_w_starlink_measurements_table(conn)
    csv_path = data_dir / csv_name
    insert_data_from_csv(conn, csv_path, countries_with_starlink_measurements_insert_query)


def _extract_alt_names(alt_str):
    if not alt_str:
        return [""] * 4
    parts = alt_str.split(",")
    return parts[:4] + [""] * (4 - len(parts[:4]))


def populate_cities_table(conn: connection, cities_txt_name: str = 'cities15000.txt', admin1_txt_name: str = 'admin1CodesASCII.txt') -> None:
    create_cities_table(conn)
    column_names = [
        "geonameid", "name", "asciiname", "alternatenames", "latitude", "longitude",
        "feature_class", "feature_code", "country_code", "cc2", "admin1_code", "admin2_code",
        "admin3_code", "admin4_code", "population", "elevation", "dem", "timezone", "modification_date"
    ]
    cities_path = data_dir / cities_txt_name
    cities_df = pd.read_csv(cities_path, sep='\t', header=None, names=column_names, dtype=str, keep_default_na=False)
    cities_df = cities_df[["country_code", "name", "asciiname", "alternatenames", "admin1_code", "admin2_code"]]

    alt_names = cities_df["alternatenames"].apply(_extract_alt_names).apply(pd.Series)
    alt_names.columns = ["name1", "name2", "name3", "name4"]

    admin1_path = data_dir / admin1_txt_name
    admin1_df = pd.read_csv(
        admin1_path, 
        sep="\t", 
        header=None, 
        names=["admin1_full_code", "admin1_name", "admin1_ascii", "geonameid"], 
        dtype=str,
        keep_default_na=False
    )

    admin1_df[["admin1_country", "admin1_code"]] = admin1_df["admin1_full_code"].str.split(".", expand=True)
    cities_df = cities_df.merge(
        admin1_df[["admin1_country", "admin1_code", "admin1_ascii"]],
        left_on=["country_code", "admin1_code"],
        right_on=["admin1_country", "admin1_code"],
        how="left"
    )

    cities_df.rename(columns={"admin1_ascii": "region"}, inplace=True)

    final_df = pd.concat([
        cities_df["name"],
        cities_df["asciiname"],
        alt_names,
        cities_df["region"],
        cities_df["country_code"]
    ], axis=1)

    final_df.columns = ["name", "asciiname", "name1", "name2", "name3", "name4", "region", "country_iso"]

    with conn.cursor() as cur:
        data_tuples = [tuple(x) for x in final_df.to_records(index=False)]
        execute_values(cur, cities_insert_query, data_tuples)
        conn.commit()