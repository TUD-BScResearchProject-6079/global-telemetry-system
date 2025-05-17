import pandas as pd
import psycopg2
import csv

column_names = [
    "geonameid", "name", "asciiname", "alternatenames", "latitude", "longitude",
    "feature_class", "feature_code", "country_code", "cc2", "admin1_code", "admin2_code",
    "admin3_code", "admin4_code", "population", "elevation", "dem", "timezone", "modification_date"
]

df = pd.read_csv("cities15000.txt", sep='\t', header=None, names=column_names, dtype=str, keep_default_na=False)
df = df[["country_code", "name", "asciiname", "alternatenames", "admin1_code", "admin2_code"]]

def extract_alt_names(alt_str):
    if not alt_str:
        return [""] * 4
    parts = alt_str.split(",")
    return parts[:4] + [""] * (4 - len(parts[:4]))

alt_names = df["alternatenames"].apply(extract_alt_names).apply(pd.Series)
alt_names.columns = ["name1", "name2", "name3", "name4"]

admin1_df = pd.read_csv(
    "admin1CodesASCII.txt", 
    sep="\t", 
    header=None, 
    names=["admin1_full_code", "admin1_name", "admin1_ascii", "geonameid"], 
    dtype=str,
    keep_default_na=False
)

admin1_df[["admin1_country", "admin1_code"]] = admin1_df["admin1_full_code"].str.split(".", expand=True)

df = df.merge(
    admin1_df[["admin1_country", "admin1_code", "admin1_ascii"]],
    left_on=["country_code", "admin1_code"],
    right_on=["admin1_country", "admin1_code"],
    how="left"
)

df.rename(columns={"admin1_ascii": "region"}, inplace=True)

final_df = pd.concat([
    df["name"],
    df["asciiname"],
    alt_names,
    df["region"],
    df["country_code"]   # add this for country_iso
], axis=1)

final_df.columns = ["name", "asciiname", "name1", "name2", "name3", "name4", "region", "country_iso"]

final_df = final_df.astype(str)
final_df.to_csv("cleaned_cities.csv", index=False, quoting=csv.QUOTE_ALL)

db_password = input("Please provide your DB password:")
conn = psycopg2.connect(
    dbname="network_measurements",
    user="postgres",
    password=db_password,
    host="localhost",
    port="5432"
)
cur = conn.cursor()

for _, row in final_df.iterrows():
    values = [None if pd.isna(x) or x.strip().lower() == "nan" else x for x in row]
    cur.execute("""
        INSERT INTO cities (name, asciiname, name1, name2, name3, name4, region, country_iso)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, tuple(row))

conn.commit()
cur.close()
conn.close()
