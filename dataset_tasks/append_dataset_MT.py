import time
import json
import requests
from terminal_colors import *
from sfdc_login import *
import math
import csv
import pandas as pd
import os
import base64
from dataset_tasks.json_metadata_generator import *
import math
import threading

def data_append_thread(dataset_name,batch_count,skiprows,job_id,server_id,access_token):

    headers = {'Authorization': "Bearer {}".format(access_token),'Content-Type': "application/json"}

    #prCyan("\r\n" + "*** Starting batch #{} ***".format(batch_count))

    load_csv_split = pd.read_csv("{}.csv".format(dataset_name), skiprows=skiprows, nrows=50000, chunksize=50000)

    export_csv = pd.concat(load_csv_split)
    export_csv = export_csv.to_csv(r"{}_dataset_split_{}.csv".format(dataset_name,batch_count), index = None, header=True, encoding='utf-8-sig')


    #prGreen("\r\n" + "Locally encoding csv batch #{} to base64.".format(batch_count))
    #time.sleep(0.15)
    ##_start = time.time()
    #Enconde csv to base64 for upload - start
    data = open("{}_dataset_split_{}.csv".format(dataset_name,batch_count), 'rb').read()
    base64_encoded = base64.b64encode(data).decode('UTF-8')
    #Enconde csv to base64 for upload - end
    #_end = time.time()
    #enc_time = round((_end-_start),2)
    #prGreen("\r\n" + "CSV encoded in {}s".format(enc_time))
    time.sleep(0.15)

    payload = {'DataFile' : '{}'.format(base64_encoded),'InsightsExternalDataId' : '{}'.format(job_id),'PartNumber': batch_count}
    payload = json.dumps(payload)

    x = 0
    xx = 0
    while x != 1:
        try:
            #prGreen("\r\n" + "Uploading file to TCRM")
            #_start = time.time()
            resp = requests.post('https://{}.salesforce.com/services/data/v51.0/sobjects/InsightsExternalDataPart'.format(server_id), headers=headers, data=payload)
            resp_results = json.loads(resp.text)
            formatted_response_str = json.dumps(resp_results, indent=2)
            #prYellow(formatted_response_str)
            try:
                success = resp_results.get('success')
            except:
                success = False
                pass
            try:
                errors = resp_results.get('errors')
            except:
                pass
            #_end = time.time()
            #upl_time = round((_end-_start),2)
            #prGreen("\r\n" + "CSV uploaded in {}s".format(upl_time))
            time.sleep(0.15)
            if success:
                #prYellow("Status: Successful")
                x += 1
                if os.path.exists("{}_dataset_split_{}.csv".format(dataset_name,batch_count)):
                    os.remove("{}_dataset_split_{}.csv".format(dataset_name,batch_count))
            else:
                #prRed(errors)
                time.sleep(0.15)
                x = 1
        except:
            xx  += 1
            time.sleep(0.15)
            if xx == 15:
                x = 1
                prRed("\r\n" + "Error in process #{}. Possible bad response from server.".format(batch_count) + "The upload won't contain all records." + "\r\n")
                try:
                    prYellow(resp)
                except:
                    pass
                try:
                    prRed(resp.text)
                except:
                    pass
                time.sleep(1)
            pass

def data_append_mp(dataset_name,skiprows,job_id,server_id,access_token,i):

    if i == 0:
        skiprows = 0
    else:
        skiprows = 50000 * i

    ind = i + 1


    headers = {'Authorization': "Bearer {}".format(access_token),'Content-Type': "application/json"}

    #prCyan("\r\n" + "*** Starting batch #{} ***".format(batch_count))

    load_csv_split = pd.read_csv("{}.csv".format(dataset_name), skiprows=skiprows, nrows=50000, chunksize=50000)

    export_csv = pd.concat(load_csv_split)
    export_csv = export_csv.to_csv(r"{}_dataset_split_{}.csv".format(dataset_name,ind), index = None, header=True, encoding='utf-8-sig')


    #prGreen("\r\n" + "Locally encoding csv batch #{} to base64.".format(batch_count))
    #time.sleep(0.15)
    ##_start = time.time()
    #Enconde csv to base64 for upload - start
    data = open("{}_dataset_split_{}.csv".format(dataset_name,ind), 'rb').read()
    base64_encoded = base64.b64encode(data).decode('UTF-8')
    #Enconde csv to base64 for upload - end
    #_end = time.time()
    #enc_time = round((_end-_start),2)
    #prGreen("\r\n" + "CSV encoded in {}s".format(enc_time))
    #time.sleep(0.015)

    payload = {'DataFile' : '{}'.format(base64_encoded),'InsightsExternalDataId' : '{}'.format(job_id),'PartNumber': ind}
    payload = json.dumps(payload)

    x = 0
    xx = 0
    while x != 1:
        try:
            #prGreen("\r\n" + "Uploading file to TCRM")
            #_start = time.time()
            resp = requests.post('https://{}.salesforce.com/services/data/v51.0/sobjects/InsightsExternalDataPart'.format(server_id), headers=headers, data=payload)
            resp_results = json.loads(resp.text)
            formatted_response_str = json.dumps(resp_results, indent=2)
            #prYellow(formatted_response_str)
            try:
                success = resp_results.get('success')
            except:
                success = False
                pass
            try:
                errors = resp_results.get('errors')
            except:
                pass
            #_end = time.time()
            #upl_time = round((_end-_start),2)
            #prGreen("\r\n" + "CSV uploaded in {}s".format(upl_time))
            time.sleep(0.15)
            if success:
                #prYellow("Status: Successful")
                x += 1
                if os.path.exists("{}_dataset_split_{}.csv".format(dataset_name,ind)):
                    os.remove("{}_dataset_split_{}.csv".format(dataset_name,ind))
            else:
                #prRed(errors)
                time.sleep(0.15)
                x = 1
        except:
            xx  += 1
            time.sleep(0.15)
            if xx == 100:
                x = 1
                prRed("\r\n" + "Error in process #{}. Possible bad response from server.".format(ind) + "The upload won't contain all records." + "\r\n")
                try:
                    prYellow(resp)
                except:
                    pass
                try:
                    prRed(resp.text)
                except:
                    pass
                time.sleep(1)
            pass

    return ind
