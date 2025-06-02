import csv
import os
from pathlib import Path

from psycopg2 import sql
from psycopg2.extensions import connection, cursor
from sql_queries import (
    cf_case_study_query,
    cf_filter_servers_study_query,
    ndt_download_case_study,
    ndt_filter_servers_study_query,
    ndt_upload_case_study,
)

path_to_case_study = (
    Path(__file__).resolve().parent.parent
    / "Case Study Distributions ndt7 and Cloudflare AIM"
    / "data"
)
path_to_output = Path(__file__).resolve().parent / "output"
cf_data = {
    "mean": "cloudflare_case_study_mean.csv",
    "median": "cloudflare_case_study_median.csv",
    "90th_percentile": "cloudflare_case_study_90th_percentile.csv",
}
ndt_data = {
    "download": "ndt7_download_case_study.csv",
    "upload": "ndt7_upload_case_study.csv",
}


def prepare_data_for_case_study(conn: connection, countries: list[str]) -> None:
    country_literals = sql.SQL(", ").join(sql.Literal(c) for c in countries)
    columns: list[str] = []
    with conn.cursor() as cur:
        for cf_table, cf_csv in cf_data.items():
            query = cf_case_study_query.format(sql.Identifier(f"cf_{cf_table}"), country_literals)
            cur.execute(query)
            if cur.description is not None:
                columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()

            full_path = os.path.join(path_to_case_study, cf_csv)
            with open(full_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(columns)
                writer.writerows(rows)

        query = ndt_download_case_study.format(country_literals)
        cur.execute(query)
        if cur.description is not None:
            columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        full_path = os.path.join(path_to_case_study, ndt_data["download"])
        with open(full_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)

        query = ndt_upload_case_study.format(country_literals)
        cur.execute(query)
        if cur.description is not None:
            columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        full_path = os.path.join(path_to_case_study, ndt_data["upload"])
        with open(full_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)


def save_cf_filtered_servers_results(conn: connection) -> None:
    with conn.cursor() as cur:
        cur.execute(cf_filter_servers_study_query)
        cf_rows = cur.fetchall()
        if cur.description is None:
            raise ValueError("No data returned from Cloudflare filter servers query.")
        cf_columns = [desc[0] for desc in cur.description]
        cf_output_path = path_to_output / "cloudflare_filtered_servers_results.csv"
        with open(cf_output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(cf_columns)
            writer.writerows(cf_rows)
    print(f"Cloudflare filtered servers results saved to {cf_output_path}")


def save_ndt_filtered_servers_results(conn: connection) -> None:
    with conn.cursor() as cur:
        cur.execute(ndt_filter_servers_study_query)
        ndt_rows = cur.fetchall()
        if cur.description is None:
            raise ValueError("No data returned from Cloudflare filter servers query.")
        ndt_columns = [desc[0] for desc in cur.description]
        ndt_output_path = path_to_output / "ndt_filtered_servers_results.csv"
        with open(ndt_output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(ndt_columns)
            writer.writerows(ndt_rows)
    print(f"NDT filtered servers results saved to {ndt_output_path}")
