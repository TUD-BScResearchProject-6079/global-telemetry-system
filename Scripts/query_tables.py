import csv
import os

from pathlib import Path
from psycopg2 import sql
from psycopg2.extensions import connection
from sql_queries import cf_case_study_query, ndt_upload_case_study, ndt_download_case_study, top_five_isps_countries_with_starlink_measurements


path_to_save = Path(__file__).resolve().parent.parent / "Case Study Distributions ndt7 and Cloudflare AIM" / "data"
cf_data = {'mean': 'cloudflare_case_study_mean.csv', 'median': 'cloudflare_case_study_median.csv', '90th_percentile': 'cloudflare_case_study_90th_percentile.csv'}
ndt_data = {'download': 'ndt7_download_case_study.csv', 'upload': 'ndt7_upload_case_study.csv'}


def prepare_data_for_case_study(conn: connection, countries: list[str]) -> None:
    country_literals = sql.SQL(', ').join(sql.Literal(c) for c in countries)
    with conn.cursor() as cur:
        for cf_table, cf_csv in cf_data.items():
            query = cf_case_study_query.format(sql.Identifier(f'cf_{cf_table}'), country_literals)
            cur.execute(query)
            columns: list[str] = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

            full_path = os.path.join(path_to_save, cf_csv)
            with open(full_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)
    
        query = ndt_download_case_study.format(country_literals)
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        full_path = os.path.join(path_to_save, ndt_data['download'])
        with open(full_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)
        
        query = ndt_upload_case_study.format(country_literals)
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        full_path = os.path.join(path_to_save, ndt_data['upload'])
        with open(full_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)

def get_top_five_isps_countries_with_starlink_measurements(conn: connection)-> None:
     with conn.cursor() as cur:
        cur.execute(top_five_isps_countries_with_starlink_measurements)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()