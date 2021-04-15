import json
import requests
from terminal_colors import *
from sfdc_login import *
import time

def get_datasets_field_details(access_token,dataset_,server_id):
    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }
    resp = requests.get('https://{}.salesforce.com/services/data/v51.0/wave/datasets/{}'.format(server_id,dataset_), headers=headers)

    formatted_response = json.loads(resp.text)
    #print(formatted_response)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    #prGreen(formatted_response_str)


    dataset_current_version_url = formatted_response.get('currentVersionUrl')
    dataset_currentVersionId = formatted_response.get('currentVersionId')

    saql = "q = load \"{}/{}\";q = group  q by all;q = foreach q generate count() as 'count';q = limit q 1;".format(dataset_,dataset_currentVersionId)

    saql_payload = {"name": "get_rows","query": str(saql), "queryLanguage": "SAQL"}

    saql_payload = json.dumps(saql_payload)

    headers = {'Authorization': "Bearer {}".format(access_token),
               'Content-Type': "application/json"
               }


    resp = requests.post('https://{}.salesforce.com/services/data/v51.0/wave/query'.format(server_id), headers=headers, data=saql_payload)
    query_results = json.loads(resp.text)
    #query_results = json.loads(resp, indent=2)
    #prYellow(query_results)
    count_rows = query_results.get('results')
    count_rows = count_rows['records']
    count_rows = count_rows[0]
    count_rows = count_rows.get('count')

    dataset_current_version_url = "https://{}.salesforce.com".format(server_id) + "{}".format(dataset_current_version_url) + "/xmds/main"

    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }

    resp = requests.get('{}'.format(dataset_current_version_url), headers=headers)
    formatted_response = json.loads(resp.text)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    #prYellow(formatted_response_str)

    try:
        dates_counter = 0
        dates_list = formatted_response.get('dates')
        prYellow("\r\n" + "Dates:")
        for x in dates_list:
            dates_counter += 1
            print("{} - ".format(dates_counter) ,"alias: ",x["alias"]," - type: ",x["type"])
        if dates_counter == 0:
            prRed("there are no dates present in the dataset.")
    except ValueError:
        prRed("there are no dates present in the dataset.")

    time.sleep(2)

    try:
        measures_list = formatted_response.get('measures')
        measures_counter = 0
        prYellow("\r\n" + "Measures:")
        for x in measures_list:
            measures_counter += 1
            print("{} - ".format(measures_counter) ,"field: ",x["field"]," - label: ",x["label"])
        if measures_counter == 0:
            prRed("there are no measures present in the dataset.")
    except ValueError:
        prRed("there are no measures present in the dataset.")

    time.sleep(2)

    try:
        dimension_list = formatted_response.get('dimensions')
        dimension_counter = 0
        prYellow("\r\n" + "Dimensions:")
        for x in dimension_list:
            dimension_counter += 1
            print("{} - ".format(dimension_counter) ,"field: ",x["field"]," - label: ",x["label"])
        if dimension_counter == 0:
            prRed("there are no dimensions present in the dataset.")
    except ValueError:
        prRed("there are no dimensions present in the dataset.")

    time.sleep(2)

    prCyan("\r\n" + "{} date fields - {} dimensions - {} measures - {} Total Records".format(dates_counter,dimension_counter,measures_counter,count_rows) + "\r\n")
