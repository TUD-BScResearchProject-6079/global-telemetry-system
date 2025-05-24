import os

from dotenv import load_dotenv
from populate_tables import (
    populate_airport_codes_table,
    populate_caida_asn_table,
    populate_cf_servers_table,
    populate_cf_tables,
    populate_cities_table,
    populate_countries_w_starlink_measurements_table,
    populate_ndt_servers_table,
    populate_ndt_table,
)
import psycopg2
from query_caida_for_as_data import download_list
from query_tables import prepare_data_for_case_study

load_dotenv()
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
    port=os.getenv("DB_PORT"),
)

countries_list = [
    "AR",
    "US",
    "CA",
    "BR",
    "GH",
    "KE",
    "FR",
    "DE",
    "PH",
    "JP",
    "AU",
    "NZ",
]


def print_menu() -> None:
    print(
        """
        Choose an operation:
        1 - Download CAIDA ASN Data
        2 - Populate ASN Table
        3 - Populate Countries With Starlink Measurements
        4 - Populate Cities and Regions Table.
        5 - Populate Airport Codes table
        6 - Populate NDT7 Servers Table
        7 - Populate Cloudflare AIM Servers Table
        8 - Populate NDT7 Table
        9 - Populate Cloudflare AIM Tables
        10 - Create Data for Case Study
        0 - Exit
    """
    )


print_menu()
while True:
    choice = input("Enter your choice: ")
    if choice == "1":
        download_list()
    elif choice == "2":
        populate_caida_asn_table(conn)
    elif choice == "3":
        populate_countries_w_starlink_measurements_table(conn)
    elif choice == "4":
        populate_cities_table(conn)
    elif choice == "5":
        populate_airport_codes_table(conn)
    elif choice == "6":
        populate_ndt_servers_table(conn)
    elif choice == "7":
        populate_cf_servers_table(conn)
    elif choice == "8":
        populate_ndt_table(conn)
    elif choice == "9":
        populate_cf_tables(conn)
    elif choice == "10":
        prepare_data_for_case_study(conn, countries_list)
    elif choice == "0":
        print("Exiting program.")
        break
    else:
        print("Invalid choice. Please enter a number from the menu.")

conn.close()
