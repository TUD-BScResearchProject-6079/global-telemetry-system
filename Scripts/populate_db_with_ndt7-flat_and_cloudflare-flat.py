import os
import gc
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

data_info = {
    "host": "localhost",
    "dbname": "network_measurements",
    "user": "postgres",
    "password": "",
    "port": "5432",
    "csv_directory_ndt": "",
    "csv_directory_cloudflare": ""
}

if (data_info["password"] == ""):
    data_info["password"] = input("Please provide the DB password: ")

if (data_info["csv_directory_ndt"] == ""):
    data_info["csv_directory_ndt"] = input("Please provide the path to the ndt7 data directory:")

if (data_info["csv_directory_cloudflare"] == ""):
    data_info["csv_directory_cloudflare"] = input("Please provide the path to the Cloudflare data directory:")

conn = psycopg2.connect(
    host=data_info["host"],
    dbname=data_info["dbname"],
    user=data_info["user"],
    password=data_info["password"],
    port=data_info["port"]
)

ndt_query = sql.SQL("""
            INSERT INTO ndt7_flat (
                uuid,
                test_time, 
                city,
                region,
                country_iso,
                asn,
                packet_loss_rate,
                throughput_mbps,
                download_latency_ms,
                download_jitter,
                download_time_ms,
                download_bytes,
                upload_latency_ms,
                upload_jitter,
                upload_time_ms,
                upload_bytes
            ) VALUES %s
        """)

cloudflare_query = sql.SQL("""
            INSERT INTO cloudflare_flat (
                uuid,
                test_time,
                city,
                region,
                country_iso,
                asn,
                packet_loss_rate,
                download_throughput_mbps,
                download_latency_ms,
                download_jitter,
                download_bytes,
                upload_throughput_mbps,
                upload_latency_ms,
                upload_jitter,
                upload_bytes
            ) VALUES %s
        """)

cur = conn.cursor()
files_with_errors = set()

def insert_data_from_csv(csv_file, insert_query):
    try:
        df = pd.read_csv(csv_file, dtype=str, keep_default_na=False)
        df = df.replace('', None)
        df = df.where(pd.notnull(df), None)

        data_tuples = [tuple(x) for x in df.to_records(index=False)]
        execute_values(cur, insert_query, data_tuples)

        conn.commit()
        print(f"Successfully inserted data from {csv_file}")

    except Exception as e:
        files_with_errors.add(csv_file)
        print(f"Error inserting data from {csv_file}: {e}")
        conn.rollback()

    finally:
        del df
        gc.collect()

for filename in os.listdir(data_info["csv_directory_ndt"]):
    if filename.endswith(".csv"):
        csv_file_path = os.path.join(data_info["csv_directory_ndt"], filename)
        insert_data_from_csv(csv_file_path, ndt_query)

for filename in os.listdir(data_info["csv_directory_cloudflare"]):
    if filename.endswith(".csv"):
        csv_file_path = os.path.join(data_info["csv_directory_cloudflare"], filename)
        insert_data_from_csv(csv_file_path, cloudflare_query)

if len(files_with_errors) > 0:
    print("\n\nErrors found in the following files:\n\n")
    print(files_with_errors)

cur.close()
conn.close()
