import time, json, requests, math, csv, os, base64, threading, pandas as pd, traceback, gc
from misc_tasks.terminal_colors import *
from misc_tasks.sfdc_login import *
from dataset_tasks.json_metadata_generator import *

def data_append_thread(dataset_name,batch_count,skiprows,job_id,server_id,access_token):

    headers = {'Authorization': "Bearer {}".format(access_token),'Content-Type': "application/json"}

    load_csv_split = pd.read_csv("{}.csv".format(dataset_name), low_memory=False, skiprows=skiprows, nrows=55000, chunksize=55000)

    export_csv = pd.concat(load_csv_split)
    export_csv = export_csv.to_csv(r"{}_dataset_split_{}.csv".format(dataset_name,batch_count), index = None, header=True, encoding='utf-8-sig')


    #Enconde csv to base64 for upload - start
    data = open("{}_dataset_split_{}.csv".format(dataset_name,batch_count), 'rb').read()
    base64_encoded = base64.b64encode(data).decode('UTF-8')
    #Enconde csv to base64 for upload - end
    time.sleep(0.15)

    payload = {'DataFile' : '{}'.format(base64_encoded),'InsightsExternalDataId' : '{}'.format(job_id),'PartNumber': batch_count}
    payload = json.dumps(payload)

    x = 0
    xx = 0
    while x != 1:
        try:
            resp = requests.post('https://{}.salesforce.com/services/data/v53.0/sobjects/InsightsExternalDataPart'.format(server_id), headers=headers, data=payload)
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

def data_append_mp(dataset_name,skiprows,job_id,server_id,access_token,i,csv_cols,server_domain,work_rows):

    if i == 0:
        skiprows = 0
    else:
        skiprows = work_rows * i

    ind = i + 1

    if work_rows >= 250000 and i < 11:
        time.sleep(i*(i*0.75))
    elif work_rows >= 250000 and i > 10:
        time.sleep(((i+20)*2))
    elif work_rows >= 250000 and i > 20:
        time.sleep(((i+30)*3))
    elif work_rows >= 250000 and i > 30:
        time.sleep(((i+40)*4))
    elif work_rows >= 250000 and i > 40:
        time.sleep(((i+50)*5))
    else:
        time.sleep(5)


    headers = {'Authorization': "Bearer {}".format(access_token),'Content-Type': "application/json"}

    load_csv_split = pd.read_csv("{}.csv".format(dataset_name), low_memory=False, skiprows=skiprows, nrows=work_rows, chunksize=work_rows, header=0, names = csv_cols)
    export_csv = pd.concat(load_csv_split)
    export_csv = export_csv.to_csv(r"{}_dataset_split_{}.csv".format(dataset_name,ind), index = None, header=True, encoding='utf-8-sig')

    #Enconde csv to base64 for upload - start
    data = open("{}_dataset_split_{}.csv".format(dataset_name,ind), 'rb').read()
    base64_encoded = base64.b64encode(data).decode('UTF-8')
    del data
    #Enconde csv to base64 for upload - end

    payload = {'DataFile' : '{}'.format(base64_encoded),'InsightsExternalDataId' : '{}'.format(job_id),'PartNumber': ind}
    payload = json.dumps(payload)
    del base64_encoded

    x = 0
    xx = 0
    while x != 1:
        try:
            try:
                resp = requests.post('https://{}.my.salesforce.com/services/data/v53.0/sobjects/InsightsExternalDataPart'.format(server_domain), headers=headers, data=payload)
                resp_results = json.loads(resp.text)
                formatted_response_str = json.dumps(resp_results, indent=2)
                success = resp_results.get('success')
            except:
                success = False
                traceback.print_exc()
                pass
            try:
                errors = resp_results.get('errors')
            except:
                traceback.print_exc()
                pass

            time.sleep(0.15)
            if success:
                x += 1
                if os.path.exists("{}_dataset_split_{}.csv".format(dataset_name,ind)):
                    os.remove("{}_dataset_split_{}.csv".format(dataset_name,ind))
            else:
                time.sleep(0.15)
                x = 1
        except:
            xx  += 1
            traceback.print_exc()
            time.sleep(10)
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

    gc.collect()
    return ind
