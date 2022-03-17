import time, json, requests, math, csv, pandas as pd, os, base64, threading
from misc_tasks.terminal_colors import *
from misc_tasks.sfdc_login import *
from dataset_tasks.json_metadata_generator import *


def new_csv_mp(dataset_name,skiprows,job_id,server_id,access_token,i,csv_cols,server_domain):

    if i == 0:
        skiprows = 0
    else:
        skiprows = 55000 * i

    ind = i + 1


    headers = {'Authorization': "Bearer {}".format(access_token),'Content-Type': "application/json"}

    load_csv_split = pd.read_csv("{}.csv".format(dataset_name), low_memory=False, skiprows=skiprows, nrows=55000, chunksize=55000, header = 0, names = csv_cols)

    export_csv = pd.concat(load_csv_split)
    export_csv.fillna(0)

    if i == 0:
        export_csv = export_csv.to_csv(r"{}_dataset_split_{}.csv".format(dataset_name,ind), index = None, header=True, encoding='utf-8-sig')
    else:
        export_csv = export_csv.to_csv(r"{}_dataset_split_{}.csv".format(dataset_name,ind), index = None, header=True, encoding='utf-8-sig')


    #Enconde csv to base64 for upload - start
    data = open("{}_dataset_split_{}.csv".format(dataset_name,ind), 'rb').read()
    base64_encoded = base64.b64encode(data).decode('UTF-8')
    #Enconde csv to base64 for upload - end

    payload = {'DataFile' : '{}'.format(base64_encoded),'InsightsExternalDataId' : '{}'.format(job_id),'PartNumber': ind}
    payload = json.dumps(payload)

    x = 0
    xx = 0
    while x != 1:
        try:
            resp = requests.post('https://{}.my.salesforce.com/services/data/v53.0/sobjects/InsightsExternalDataPart'.format(server_domain), headers=headers, data=payload)
            resp_results = json.loads(resp.text)
            formatted_response_str = json.dumps(resp_results, indent=2)
            try:
                success = resp_results.get('success')
            except:
                success = False
                pass
            try:
                errors = resp_results.get('errors')
            except:
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
            time.sleep(0.15)
            if xx == 15:
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

    return i
