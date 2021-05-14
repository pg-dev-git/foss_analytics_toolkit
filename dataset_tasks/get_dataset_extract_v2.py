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
import threading
from dataset_tasks.dataset_extract_MT import *
import time
import subprocess
from line import *
import datetime

#os.chdir("/Users/pgagliar/Desktop/api_test/")

def get_datasets_extract(access_token,dataset_,server_id,dataset_name):

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
            field = x["field"]
            if field.endswith("_epoch"):
                pass
            else:
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
            field = x["field"]
            if field.endswith("_Second") or field.endswith("_Minute") or field.endswith("_Hour") or field.endswith("_Day") or field.endswith("_Week") or field.endswith("_Month") or field.endswith("_Quarter") or field.endswith("_Year") or field.endswith("_epoch"):
                pass
            else:
                dimension_counter += 1
                query_fields.append(x["field"])
                #print(field)
        #print(type(query_fields))
    except:
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
    batches_10 = math.ceil(batches_ / 20)
    batches_100 = math.ceil(batches_ / 100)
    batches_1000 = math.ceil(batches_ / 1000)

    #multithreaded function to submit the queries
    if batches_ > 0 and batches_ < 10:
        threads = list()
        prCyan("\r\n" + "Starting {} CPU threads to extract the dataset".format(batches_) + "\r\n")
        _start = time.time()
        for index in range(batches_):
            x = threading.Thread(target=data_extract_thread, args=(dataset_,dataset_currentVersionId,query_fields_str,q_offset,q_limit,i,access_token,dataset_name,server_id,))
            threads.append(x)
            x.start()
            i += 1
            q_offset += 9999
            time.sleep(1)

        prCyan("\r\n" + "Progress: " + "\r\n")

        for index, thread in enumerate(threads):
            progress = ((index + 1) / batches_)*100
            progress = round(progress)
            if progress < 10:
                prYellow("  {}%".format(progress))
            elif progress < 30:
                prYellow(" {}%".format(progress))
            elif progress < 60:
                prLightPurple(" {}%".format(progress))
            elif progress < 100:
                prCyan(" {}%".format(progress))
            elif progress == 100:
                prGreen("{}%".format(progress))
            thread.join()
            time.sleep(0.5)

        _end = time.time()
        total_time = round((_end - _start),2)
        prGreen("\r\n" + "Multithreaded extraction completed in {}s.".format(total_time))
        line_print()
        time.sleep(1)

        #Append all csv files from the batches - start:

        prGreen("\r\n" + "Compiling CSV.")
        extension = 'csv'
        _start = time.time()
        #combined_csv = subprocess.run(["Get-ChildItem", "-Filter", "*_results.csv", "|", "Select-Object", "-ExpandProperty", "FullName", "|", "Import-Csv", "|", "Export-Csv", "-Path","{}".format(d_ext), "-NoTypeInformation"], stdout=subprocess.PIPE, text=True, shell=True, stderr=subprocess.DEVNULL)
        cmd = "Get-ChildItem -Filter *_results.csv | Select-Object -ExpandProperty FullName | Import-Csv | Export-Csv .\{}_extract.csv -NoTypeInformation".format(dataset_name)
        completed = subprocess.run(["powershell", "-Command", cmd], capture_output=True)

        _end = time.time()
        total_time = round((_end - _start),2)
        prGreen("\r\n" + "CSV compiled in {}s".format(total_time) + "\r\n")
        line_print()
        time.sleep(1)
        prCyan("\r\n" + "Dataset Succesfully Exported. Find the file here:")
        time.sleep(0.5)
        prLightPurple("\r\n" + "{}".format(d_ext) + "\r\n")
        time.sleep(0.2)
        line_print()

        #Append all csv files from the batches - end.

        #Folder Cleanup
        ii = 1
        while ii <= batches_:
            #Remove partial json files
            os.remove('{}_{}_query_results.json'.format(dataset_name,i))
            #Remove partial csv files
            os.remove('{}_{}_query_results.csv'.format(dataset_name,i))
            ii += 1

    if batches_ > 9:
        batch_count = 0
        batch_10_count = 0
        delete_count = 0
        ii = 1
        rem_jobs = batches_
        job_count = 0
        prCyan("\r\n" + "Starting extraction now... " + "\r\n")
        time.sleep(1)
        prCyan("\r\n" + "Progress: " + "\r\n")
        total_start = time.time()
        while batch_10_count <= batches_10:
            batch_10_count += 1
            delete_count += 20
            job_count = 20

            if job_count <= rem_jobs:
                rem_jobs = rem_jobs - 20
                t_count = 20
            else:
                t_count = rem_jobs
                rem_jobs = 0

            threads = list()
            #prCyan("\r\n" + "Starting {} CPU threads to extract the dataset".format(batches_) + "\r\n")
            _start = time.time()
            for index in range(t_count):
                x = threading.Thread(target=data_extract_thread, args=(dataset_,dataset_currentVersionId,query_fields_str,q_offset,q_limit,i,access_token,dataset_name,server_id,))
                threads.append(x)
                x.start()
                i += 1
                q_offset += 9999
                batch_count += 1
                time.sleep(0.1)


            for index, thread in enumerate(threads):
                thread.join()
                time.sleep(0.1)

            _end = time.time()
            total_time = round((_end - _start),2)
            #prGreen("\r\n" + "Multithreaded extraction completed in {}s.".format(total_time))
            #line_print()
            time.sleep(0.1)

            #Append all csv files from the batches - start:

            #prGreen("\r\n" + "Compiling CSV.")
            extension = 'csv'

            #combined_csv = subprocess.run(["Get-ChildItem", "-Filter", "*_results.csv", "|", "Select-Object", "-ExpandProperty", "FullName", "|", "Import-Csv", "|", "Export-Csv", "-Path","{}".format(d_ext), "-NoTypeInformation"], stdout=subprocess.PIPE, text=True, shell=True, stderr=subprocess.DEVNULL)
            cmd = "Get-ChildItem -Filter *_results.csv | Select-Object -ExpandProperty FullName | Import-Csv | Export-Csv .\{}_extract_{}_split.csv -NoTypeInformation".format(dataset_name,batch_10_count)
            completed = subprocess.run(["powershell", "-Command", cmd], capture_output=True)


            #prGreen("\r\n" + "CSV compiled in {}s".format(total_time) + "\r\n")
            #line_print()
            #time.sleep(0.25)
            #prCyan("\r\n" + "Dataset Succesfully Exported. Find the file here:")
            #time.sleep(0.5)
            #prLightPurple("\r\n" + "{}".format(d_ext) + "\r\n")
            #time.sleep(0.2)
            #line_print()

            #Append all csv files from the batches - end.

            progress = round((batch_10_count/batches_10)*100,1)

            if progress < 10:
                prYellow("  {}%".format(progress))
            elif progress < 30:
                prYellow(" {}%".format(progress))
            elif progress < 60:
                prLightPurple(" {}%".format(progress))
            elif progress < 100:
                prCyan(" {}%".format(progress))
            elif progress == 100:
                prGreen("{}%".format(progress))

            #Folder Cleanup

            while ii <= delete_count:
                try:
                    #Remove partial json files
                    os.remove('{}_{}_query_results.json'.format(dataset_name,ii))
                    #Remove partial csv files
                    os.remove('{}_{}_query_results.csv'.format(dataset_name,ii))
                    ii += 1
                except:
                    ii += 1


        #Append all csv files from the batches - start:

        prGreen("\r\n" + "Compiling CSV.")
        extension = 'csv'
        _start = time.time()
        #combined_csv = subprocess.run(["Get-ChildItem", "-Filter", "*_results.csv", "|", "Select-Object", "-ExpandProperty", "FullName", "|", "Import-Csv", "|", "Export-Csv", "-Path","{}".format(d_ext), "-NoTypeInformation"], stdout=subprocess.PIPE, text=True, shell=True, stderr=subprocess.DEVNULL)
        cmd = "Get-ChildItem -Filter *_split.csv | Select-Object -ExpandProperty FullName | Import-Csv | Export-Csv .\{}_extract.csv -NoTypeInformation".format(dataset_name)
        completed = subprocess.run(["powershell", "-Command", cmd], capture_output=True)

        _end = time.time()
        total_time = round((_end - _start),2)
        prGreen("\r\n" + "CSV compiled in {}s".format(total_time) + "\r\n")

        ii = 1
        while ii <= batch_10_count:
            try:
                #Remove partial csv files
                os.remove('{}_extract_{}_split.csv'.format(dataset_name,ii))
                ii += 1
            except:
                ii += 1

        total_end = time.time()
        total_time = round((total_end - total_start),2)
        total_time = time.strftime("%H h : %M m : %S s", time.gmtime(total_time))
        print("\r\n" + "Total Extraction Time: {}".format(total_time))
        line_print()
        time.sleep(0.25)
        prCyan("\r\n" + "Dataset Succesfully Exported. Find the file here:")
        time.sleep(0.5)
        prLightPurple("\r\n" + "{}".format(d_ext) + "\r\n")
        time.sleep(0.2)
        line_print()



    #Append all csv files from the batches - end.

    if batches_ > 99 and batches_ < 1000:
        pass

    if batches_ > 999 and batches_ < 10000:
        pass

    if batches_ > 9999:
        pass




    #Go back to parent folder:
    os.chdir("..")
    prCyan("\r\n" + "Dataset selected: {} - {}".format(dataset_name, dataset_))
