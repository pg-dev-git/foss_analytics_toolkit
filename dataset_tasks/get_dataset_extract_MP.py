import json
import requests
from terminal_colors import *
from sfdc_login import *
import math
import csv
import pandas as pd
import glob
import os
import base64
import multiprocessing as mp
from dataset_tasks.dataset_extract_MP import *
from dataset_tasks.mt_for_mp import *
import time

#os.chdir("/Users/pgagliar/Desktop/api_test/")

def get_datasets_extract_mp(access_token,dataset_,server_id):

    try:
        dataset_extraction_dir = "dataset_extraction"
        os.mkdir(dataset_extraction_dir)
    except OSError as error:
            print(" ")

    cd = os.getcwd()
    #print(cd)

    d_ext = "{}".format(cd)+"/dataset_extraction/"
    #print(d_ext)

    os.chdir(d_ext)

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
    dataset_name = formatted_response.get('name')

    saql = "q = load \"{}/{}\";q = group  q by all;q = foreach q generate count() as 'count';q = limit q 1;".format(dataset_,dataset_currentVersionId)

    saql_payload = {"name": "get_rows","query": str(saql), "queryLanguage": "SAQL"}

    saql_payload = json.dumps(saql_payload)

    headers = {'Authorization': "Bearer {}".format(access_token),
               'Content-Type': "application/json"
               }

    resp = requests.post('https://{}.salesforce.com/services/data/v51.0/wave/query'.format(server_id), headers=headers, data=saql_payload)
    query_results = json.loads(resp.text)
    count_rows = query_results.get('results')
    count_rows = count_rows['records']
    count_rows = count_rows[0]
    count_rows = count_rows.get('count')
    batches_ = math.ceil(count_rows / 9999)
    #print(batches_)

    #Folder Cleanup
    i = 0
    while i <= batches_:
        #Remove partial json files
        if os.path.exists('{}_{}_query_results.csv'.format(dataset_name,i)):
            os.remove('{}_{}_query_results.json'.format(dataset_name,i))
        #Remove partial csv files
        if os.path.exists('{}_{}_query_results.csv'.format(dataset_name,i)):
            os.remove('{}_{}_query_results.csv'.format(dataset_name,i))
        i += 1

    dataset_current_version_url = "https://{}.salesforce.com".format(server_id) + "{}".format(dataset_current_version_url) + "/xmds/main"

    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }

    resp = requests.get('{}'.format(dataset_current_version_url), headers=headers)
    formatted_response = json.loads(resp.text)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    #prYellow(formatted_response_str)

    try:
        measures_list = formatted_response.get('measures')
        query_fields=[]
        #print(measures_list)
        measures_counter = 0
        #prYellow("\r\n" + "Measures:")
        for x in measures_list:
            measures_counter += 1
            query_fields.append(x["field"])
        print("\r\n")
    except ValueError:
        prRed("there are no measures present in the dataset.")

    try:
        dimension_list = formatted_response.get('dimensions')
        dimension_counter = 0
        #prYellow("\r\n" + "Dimensions:")
        for x in dimension_list:
            dimension_counter += 1
            query_fields.append(x["field"])
        #print(type(query_fields))
    except ValueError:
        prRed("there are no dimensions present in the dataset.")

    #def convert_list_to_string(query_fields, seperator=','):
    #    return seperator.join(query_fields)

    #query_fields_str = convert_list_to_string(query_fields)

    query_fields_str = ', '.join(f'\'{w}\'' for w in query_fields)

    total_fields = dimension_counter + measures_counter
    #print(total_fields)

    i = 1
    q_limit = 9999
    q_offset = 0
    dataset_extraction_dir = "dataset_extraction"

    #multicore & threads function to submit the queries
    if batches_ > 0:
        pool = mp.Pool((mp.cpu_count()))
        cpus = int(mp.cpu_count())
        prCyan("\r\n" + "Starting extraction using {} CPU cores...".format(cpus) + "\r\n")
        mts = math.ceil(batches_ / cpus)
        pool_cycles_A = math.ceil(batches_ / mts)
        pool_cycles_B = math.floor(batches_ / mts)
        control_flag = mts
        _start = time.time()
        result_async = [pool.apply_async(data_extract_mp, args = (dataset_,dataset_currentVersionId,query_fields_str,q_offset,q_limit,i,access_token,dataset_name,server_id,batches_, )) for i in
                        range(batches_)]
        try:
            for r in result_async:
                results = r.get()
                progress = round((results / batches_) * 100)
                if progress < 10:
                    prYellow("  {}%".format(progress))
                elif progress < 30:
                    prYellow(" {}%".format(progress))
                elif progress < 60:
                    prLightPurple(" {}%".format(progress))
                elif progress < 100:
                    prCyan(" {}%".format(progress))
                time.sleep(0.5)
        except ValueError:
            pass

        #results = [r.get() for r in result_async]

        prGreen("100%")
        pool.close()
        pool.join()
        _end = time.time()
        total_time = round((_end - _start),2)
        prGreen("\r\n" + "Extraction completed in {}s.".format(total_time))
        time.sleep(1)

    if batches_ > 0:
        #Folder check for existing files - start:
        if os.path.exists("{}_dataset_extraction.csv".format(dataset_name)):
            os.remove("{}_dataset_extraction.csv".format(dataset_name))

        if os.path.exists('{}_{}_query_results.json'.format(dataset_name,i)):
            os.remove('{}_{}_query_results.json'.format(dataset_name,i))

        if os.path.exists('{}_{}_query_results.csv'.format(dataset_name,i)):
            os.remove('{}_{}_query_results.csv'.format(dataset_name,i))
        #Folder check for existing files - end.

        #Append all csv files from the batches - start:

        prGreen("\r\n" + "Compiling CSV.")
        extension = 'csv'
        csv_files = glob.glob('{}_*.{}'.format(dataset_name,extension))
        combined_csv = pd.concat([pd.read_csv(csv_file) for csv_file in csv_files])
        #print(combined_csv)
        combined_csv.to_csv( "{}_dataset_extraction.csv".format(dataset_name), index=False, encoding='utf-8-sig')
        prCyan("\r\n" + "Dataset Succesfully Exported." + "\r\n")
        time.sleep(0.5)
        prCyan("\r\n" + "Find the file here: {}".format(d_ext) + "\r\n")
        time.sleep(0.2)

        #Append all csv files from the batches - end.


    #Folder Cleanup
    i = 0
    while i <= batches_:
        #Remove partial json files
        if os.path.exists('{}_{}_query_results.csv'.format(dataset_name,i)):
            os.remove('{}_{}_query_results.json'.format(dataset_name,i))
        #Remove partial csv files
        if os.path.exists('{}_{}_query_results.csv'.format(dataset_name,i)):
            os.remove('{}_{}_query_results.csv'.format(dataset_name,i))
        i += 1

    #Go back to parent folder:
    os.chdir("..")
