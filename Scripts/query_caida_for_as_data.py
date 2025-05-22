import csv
import json
import sys

from populate_tables import data_dir
from graphqlclient import GraphQLClient
from typing import Callable


URL = "https://api.asrank.caida.org/v2/graphql"
PAGE_SIZE = 10000
column_names = ["asn", "asnName", "rank", "country_code", "country_name"]
decoder = json.JSONDecoder()

def asn_query(first: int, offset: int) -> str:
    return [
        "asns",
        """{
            asns(first:%s, offset:%s) {
                totalCount
                pageInfo {
                    first
                    hasNextPage
                }
                edges {
                    node {
                        asn
                        asnName
                        rank
                        country {
                            iso
                            name
                        }
                    }
                }
            }
        }""" % (first, offset)
    ]


def download_data(url, query):
    client = GraphQLClient(url)
    return decoder.decode(client.execute(query))


def download_list(url: str = URL, fname:str = "asns_query_results.csv", api_query: Callable[[int, int], str] = asn_query):
    hasNextPage = True
    first = PAGE_SIZE
    offset = 0

    path = data_dir / fname
    print("Writing to", path)
    with open(path, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=column_names)
        writer.writeheader()

        while hasNextPage:
            type, query = api_query(first, offset)
            data = download_data(url, query)

            if not ("data" in data and type in data["data"]):
                print("Failed to parse:", data, file=sys.stderr)
                sys.exit()

            data = data["data"][type]
            for node in data["edges"]:
                n = node["node"]
                row = {
                    "asn": n.get("asn"),
                    "asnName": n.get("asnName"),
                    "rank": n.get("rank"),
                    "country_code": n.get("country", {}).get("iso"),
                    "country_name": n.get("country", {}).get("name"),
                }
                writer.writerow(row)

            hasNextPage = data["pageInfo"]["hasNextPage"]
            offset += data["pageInfo"]["first"]
