import json, requests, math, glob, os, time, sys, subprocess, configparser, threading, csv, base64, datetime, gc, pandas as pd
from misc_tasks.terminal_colors import *
from misc_tasks.sfdc_login import *
from misc_tasks.line import *

def control_files(access_token,dataset_,server_id,yy,batches_mt,thread_id,cpus_required,cpus):

    if os.path.exists("mp{}.ini".format(yy)) == False:
        if (yy + 1) == cpus_required and cpus_required != 1:
            last = 'Y'
        else:
            last = 'N'
        config = configparser.ConfigParser()
        config['DEFAULT'] = {'thread_id': '{}'.format(thread_id),'batches_mt': '{}'.format(batches_mt), 'last': '{}'.format(last)}

        with open("mp{}.ini".format(yy), 'w') as configfile:
            config.write(configfile)

        configfile.close()

    elif os.path.exists("mp{}.ini".format(yy)):
        os.remove("mp{}.ini".format(yy))
        if (yy + 1) == cpus_required:
            last = 'Y'
        else:
            last = 'N'
        config = configparser.ConfigParser()
        config['DEFAULT'] = {'thread_id': '{}'.format(thread_id),'batches_mt': '{}'.format(batches_mt), 'last': '{}'.format(last)}

        with open("mp{}.ini".format(yy), 'w') as configfile:
            config.write(configfile)

        configfile.close()


def mp_threads(access_token,dataset_,server_id,dataset_name,thread_id,dataset_currentVersionId,query_fields_str,q_limit,batch_count,server_domain,work_rows):

    time.sleep(0.01)

    if thread_id == 0:
        q_offset = 0
    else:
        q_offset = work_rows * thread_id

    saql = "q = load \"{}/{}\";q = foreach q generate {};q = offset q {};q = limit q {};".format(dataset_,dataset_currentVersionId,query_fields_str,q_offset,q_limit)
    saql_payload = {"name": "get_rows","query": str(saql), "queryLanguage": "SAQL"}
    saql_payload = json.dumps(saql_payload)


    headers = {'Authorization': "Bearer {}".format(access_token),
               'Content-Type': "application/json"
               }
    x = 0
    xx = 0
    while x != 1:


        try:
            resp = requests.post('https://{}.my.salesforce.com/services/data/v53.0/wave/query'.format(server_domain), headers=headers, data=saql_payload)
            resp_text = json.loads(resp.text)
            try:
                query_results = ((resp_text.get("results").get("records")))
                query_results_json = json.dumps(query_results)
                df = pd.read_json(query_results_json)
                df.fillna(0)
                export_csv = df.to_csv(r'{}_{}_query_results.csv'.format(dataset_name,thread_id), index = None, header=True, encoding='utf-8')

                del export_csv
                gc.collect()
                x = 1
            except:
                time.sleep(3)

        except:
            xx += 1
            time.sleep(15)
            traceback.print_exc()
            if xx == 150:
                x = 1
                prRed("\r\n" + "Error in process #{}. Possible bad response from server.".format(thread_id) + "The file won't contain all records." + "\r\n")
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
