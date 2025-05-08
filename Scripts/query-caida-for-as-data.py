import sys
import json
import csv
from graphqlclient import GraphQLClient

URL = "https://api.asrank.caida.org/v2/graphql"
PAGE_SIZE = 10000
decoder = json.JSONDecoder()

def main():
    DownloadList(URL, "asns_query_results.csv", AsnsQuery)

def DownloadList(url, fname, function):
    hasNextPage = True
    first = PAGE_SIZE
    offset = 0

    print("Writing to", fname)
    with open(fname, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "asn", "asnName", "rank",
            "country_iso", "country_name",
            "numberPrefixes", "numberAddresses"
        ])
        writer.writeheader()

        while hasNextPage:
            type, query = function(first, offset)
            data = DownloadQuery(url, query)

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
                    "country_iso": n.get("country", {}).get("iso"),
                    "country_name": n.get("country", {}).get("name"),
                    "numberPrefixes": n.get("announcing", {}).get("numberPrefixes"),
                    "numberAddresses": n.get("announcing", {}).get("numberAddresses")
                }
                writer.writerow(row)

            hasNextPage = data["pageInfo"]["hasNextPage"]
            offset += data["pageInfo"]["first"]

def DownloadQuery(url, query):
    client = GraphQLClient(url)
    return decoder.decode(client.execute(query))

def AsnsQuery(first, offset):
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
                        announcing {
                            numberPrefixes
                            numberAddresses
                        }
                    }
                }
            }
        }""" % (first, offset)
    ]

if __name__ == "__main__":
    main()
