import json
import requests
from terminal_colors import *
from sfdc_login import *
import time
import pandas as pd
import csv
from line import *

def get_datasets_field_details(access_token,dataset_,server_id,dataset_name):

    try:
        dataset_extraction_dir = "dataset_extraction"
        os.mkdir(dataset_extraction_dir)
    except OSError as error:
            print(" ")

    cd = os.getcwd()
    #print(cd)

    d_ext = "{}".format(cd)+"\\dataset_extraction\\"
    #print(d_ext)

    os.chdir(d_ext)

    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }
    resp = requests.get('https://{}.salesforce.com/services/data/v51.0/wave/datasets/{}'.format(server_id,dataset_), headers=headers)

    formatted_response = json.loads(resp.text)
    #print(formatted_response)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    prGreen(formatted_response_str)


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

    prGreen("\r\n" + "Getting fields...")
    time.sleep(1)
    fields = []
    print("\r\n")

    try:
        dates_counter = 0
        dates_list = formatted_response.get('dates')
        #prYellow("\r\n" + "Dates:")
        for x in dates_list:
            dates_counter += 1
            #print("{} - ".format(dates_counter) ,"alias: ",x["alias"]," - type: ",x["type"])
            fields.append(([x["alias"],x["label"],'date']))
        if dates_counter == 0:
            prRed("there are no dates present in the dataset.")
    except:
        pass

    time.sleep(2)

    try:
        measures_list = formatted_response.get('measures')
        measures_counter = 0
        #prYellow("\r\n" + "Measures:")
        for x in measures_list:
            field = x["field"]
            if field.endswith("_epoch"):
                pass
            else:
                measures_counter += 1
                #print("{} - ".format(measures_counter) ,"field: ",x["field"]," - label: ",x["label"])
                fields.append(([x["field"],x["label"],'measure']))
        if measures_counter == 0:
            prRed("there are no measures present in the dataset.")
    except:
        pass

    time.sleep(2)

    try:
        dimension_list = formatted_response.get('dimensions')
        dimension_counter = 0
        #prYellow("\r\n" + "Dimensions:")
        for x in dimension_list:
            field = x["field"]
            if field.endswith("_Second") or field.endswith("_Minute") or field.endswith("_Hour") or field.endswith("_Day") or field.endswith("_Week") or field.endswith("_Month") or field.endswith("_Quarter") or field.endswith("_Year") or field.endswith("_epoch"):
                pass
            else:
                dimension_counter += 1
                #print("{} - ".format(dimension_counter) ,"field: ",x["field"]," - label: ",x["label"])
                fields.append(([x["field"],x["label"],'dimension']))
        if dimension_counter == 0:
            prRed("there are no dimensions present in the dataset.")
    except:
        pass

    df = pd.DataFrame(fields,columns=['API Name','Label','Type'])

    print(df)

    time.sleep(0.5)

    prCyan("\r\n" + "{} date fields - {} dimensions - {} measures - {} Total Records".format(dates_counter,dimension_counter,measures_counter,count_rows) + "\r\n")

    line_print()

    time.sleep(0.5)

    user_input = input("\r\n" + "Do you want to export this list as a csv file? (Y/N): ")

    time.sleep(1)

    if user_input == "y" or user_input == "Y":

        fields = []

        for x in dimension_list:
            field = x["field"]
            if field.endswith("_Second") or field.endswith("_Minute") or field.endswith("_Hour") or field.endswith("_Day") or field.endswith("_Week") or field.endswith("_Month") or field.endswith("_Quarter") or field.endswith("_Year") or field.endswith("_epoch"):
                pass
            else:
                fields.append(([x["field"],x["label"],'dimension']))

        for x in measures_list:
            field = x["field"]
            if field.endswith("_epoch"):
                pass
            else:
                fields.append(([x["field"],x["label"],'measure']))

        for x in dates_list:
                fields.append(([x["alias"],x["label"],'date']))

        df = pd.DataFrame(fields,columns=['field_api_name','field_label','type'])

        df.to_csv('{}_field_list_export.csv'.format(dataset_name), index=False)

        prGreen("\r\n" + "File successfuly exported here:")
        prLightPurple("\r\n" + "{}".format(d_ext))
        line_print()

        time.sleep(1)

        os.chdir("..")
        prCyan("\r\n" + "Dataset selected: {} - {}".format(dataset_name, dataset_))

    else:
        line_print()
        time.sleep(1)
        os.chdir("..")
        prCyan("\r\n" + "Dataset selected: {} - {}".format(dataset_name, dataset_))
