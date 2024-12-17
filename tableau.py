import json
import logging
import argparse
import getpass
import requests
import csv
from datetime import time
import tableauserverclient as TSC

def lambda_handler(event, context):
    # logging.basicConfig(level=logging.INFO)
    # logging.info("Process started")
    
    try:
        dashboards = main()
        # print(type(dashboards), dashboards)
        process_dashboards(dashboards)
        return {
            'statusCode': 200,
            'body': json.dumps('Processed Successfully')
        }
    except Exception as e:
        logging.error(f"Error occurred: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error occurred: {e}")
        }

def main():
    query = """
    {
        dashboards(filter: {name: "Resource Search"}) {
            id
            name
            upstreamDatasources {
                id
                name
            }upstreamTables{
                id
                name
            }upstreamColumns{
                id
                name
            }
        }
    }
    """
    v_server_url = 'https://prod-useast-b.online.tableau.com'
    v_site = 'allegistableauonline'
    v_token_name = 'tableau_metadata'
    v_token_secret = 'Pbh7ekYqSfqpPSsKdNy0hQ==:U2OFZfmQ9q3n6Rrz295l16tqdEu6ivuK'
    server = TSC.Server(v_server_url, use_server_version=True)
    tableau_auth = TSC.PersonalAccessTokenAuth(token_name=v_token_name, personal_access_token=v_token_secret, site_id=v_site)
    with server.auth.sign_in_with_personal_access_token(tableau_auth):
        print('[Logged in successfully to {}]'.format(v_server_url))
        resp = server.metadata.query(query)
        return resp['data']

def process_dashboards(data):
    rows = []
    for dashboard in data['dashboards']:
        row = {
            "Dashboard ID": dashboard['id'],
            "Name": dashboard['name']
        }
        for datasource in dashboard["upstreamDatasources"]:
            row["Upstream Datasource ID"] = datasource['id']
            row["Upstream Datasource Name"] = datasource['name']
        for table in dashboard["upstreamTables"]:
            row["Upstream Table ID"] = table['id']
            row["Upstream Table Name"] = table['name']
        for column in dashboard["upstreamColumns"]:
            row["Upstream Column ID"] = column['id']
            row["Upstream Column Name"] = column['name']
        rows.append(row)
    print(rows)
    
    write_to_csv(rows)


def write_to_csv(rows):
    with open('tableau_metadata.csv', mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=rows[0].keys())
        writer.writeheader()
        for row in rows:
            writer.writerow(row)