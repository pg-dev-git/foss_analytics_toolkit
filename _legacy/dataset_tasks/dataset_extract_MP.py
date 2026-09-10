import json, requests, csv, pandas as pd, time, os
from misc_tasks.terminal_colors import *


def data_extract_mp(dataset_,dataset_currentVersionId,query_fields_str,q_offset,q_limit,i,access_token,dataset_name,server_id,batches_,query_fields):

    time.sleep(0.1)

    if i == 0:
        q_offset = 0
    else:
        q_offset = 9999 * i

    saql = "q = load \"{}/{}\";q = foreach q generate {};q = offset q {};q = limit q {};".format(dataset_,dataset_currentVersionId,query_fields_str,q_offset,q_limit)
    saql_payload = {"name": "get_rows","query": str(saql), "queryLanguage": "SAQL"}
    saql_payload = json.dumps(saql_payload)

    headers = {'Authorization': "Bearer {}".format(access_token),
               'Content-Type': "application/json"
               }
    x = 0
    xx = 0
    while x != 1:
        resp = requests.post('https://{}.salesforce.com/services/data/v53.0/wave/query'.format(server_id), headers=headers, data=saql_payload)
        query_results = json.loads(resp.text)
        try:
            query_results = ((query_results.get("results").get("records")))
            query_results_json = json.dumps(query_results)

            with open('{}_{}_query_results.json'.format(dataset_name,i), 'w') as outfile:
                json.dump(query_results, outfile)

            outfile.close()

            df = pd.read_json(r'{}_{}_query_results.json'.format(dataset_name,i))
            df.fillna(0)
            export_csv = df.to_csv(r'{}_{}_query_results.csv'.format(dataset_name,i), index = None, header=True, encoding='utf-8')

            time.sleep(0.1)
            x = 1
        except:
            xx += 1
            if xx == 5:
                x = 1
                prRed("\r\n" + "Error in process #{}. Possible bad response from server.".format(i) + "\r\n")
                time.sleep(2)
            pass

    try:
        if os.path.exists('{}_{}_query_results.json'.format(dataset_name,i)):
            os.remove('{}_{}_query_results.json'.format(dataset_name,i))
    except:
        pass

    return i
