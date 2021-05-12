import json
import requests
from terminal_colors import *
import csv
import pandas as pd
import time

def data_extract_mp(dataset_,dataset_currentVersionId,query_fields_str,q_offset,q_limit,i,access_token,dataset_name,server_id,batches_,query_fields):

    time.sleep(0.2)

    #prRed("Starting MP Function now.")

    #print(i)
    if i == 0:
        q_offset = 0
    else:
        q_offset = 9999 * i


    #prRed("Building query")
    saql = "q = load \"{}/{}\";q = foreach q generate {};q = offset q {};q = limit q {};".format(dataset_,dataset_currentVersionId,query_fields_str,q_offset,q_limit)
    #print(saql)

    #prRed("Building payload")
    saql_payload = {"name": "get_rows","query": str(saql), "queryLanguage": "SAQL"}

    #prRed("dumping payload")
    saql_payload = json.dumps(saql_payload)

    headers = {'Authorization': "Bearer {}".format(access_token),
               'Content-Type': "application/json"
               }
    x = 0
    while x != 1:
        #prRed("sending requst")
        resp = requests.post('https://{}.salesforce.com/services/data/v51.0/wave/query'.format(server_id), headers=headers, data=saql_payload)
        #prRed("Loading results")
        query_results = json.loads(resp.text)
        formatted_response_str = json.dumps(query_results, indent=2)
        #prYellow(formatted_response_str)
        try:
            query_results = ((query_results.get("results").get("records")))
            query_results_json = json.dumps(query_results)
            #print(query_results_json)
            listOfDicts = query_results
            #keys = [i for s in [d.keys() for d in listOfDicts] for i in s]
            keys = query_fields
            #print(keys)


            #print("writing csv")
            #with open('{}_{}_query_results.csv'.format(dataset_name,i), 'w') as output_file:
            #    dict_writer = csv.DictWriter(output_file, restval="-", fieldnames=keys, delimiter=',')
            #    dict_writer.writeheader()
            #    dict_writer.writerows(listOfDicts)

            #output_file.close()
            #print("csv done")


            with open('{}_{}_query_results.json'.format(dataset_name,i), 'w') as outfile:
                json.dump(query_results, outfile)

            outfile.close()

            df = pd.read_json(r'{}_{}_query_results.json'.format(dataset_name,i))
            df.fillna(0)
            export_csv = df.to_csv(r'{}_{}_query_results.csv'.format(dataset_name,i), index = None, header=True, encoding='utf-8')

            #if os.path.exists('{}_{}_query_results.json'.format(dataset_name,i)):
            #    os.remove('{}_{}_query_results.json'.format(dataset_name,i))

            #prGreen("Thread #{} finished.".format(i))
            time.sleep(0.2)
            x = 1
        except:
            pass
    return i
