import json
import requests
from terminal_colors import *
import csv
import pandas as pd
import time

def data_extract_mp(dataset_,dataset_currentVersionId,query_fields_str,q_offset,q_limit,i,access_token,dataset_name,server_id,batches_):

    time.sleep(1)

    if i == 0:
        q_offset = 0
    else:
        q_offset = 9999 * i


    saql = "q = load \"{}/{}\";q = foreach q generate {};q = offset q {};q = limit q {};".format(dataset_,dataset_currentVersionId,query_fields_str,q_offset,q_limit)
    #print(saql)

    saql_payload = {"name": "get_rows","query": str(saql), "queryLanguage": "SAQL"}

    saql_payload = json.dumps(saql_payload)

    headers = {'Authorization': "Bearer {}".format(access_token),
               'Content-Type': "application/json"
               }
    x = 0
    while x != 1:
        resp = requests.post('https://{}.salesforce.com/services/data/v51.0/wave/query'.format(server_id), headers=headers, data=saql_payload)
        query_results = json.loads(resp.text)
        formatted_response_str = json.dumps(query_results, indent=2)
        #prYellow(formatted_response_str)
        try:
            query_results = ((query_results.get("results").get("records")))
            query_results_json = json.dumps(query_results)

            with open('{}_{}_query_results.json'.format(dataset_name,i), 'w') as outfile:
                json.dump(query_results, outfile)

            df = pd.read_json(r'{}_{}_query_results.json'.format(dataset_name,i))
            export_csv = df.to_csv(r'{}_{}_query_results.csv'.format(dataset_name,i), index = None, header=True)

            #prGreen("Thread #{} finished.".format(i))
            time.sleep(1)
            x = 1
        except AttributeError:
            pass
    return i
