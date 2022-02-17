import json, requests, math, glob, os, time, sys, subprocess, configparser, threading, csv, base64, datetime, gc
from terminal_colors import *
from sfdc_login import *
from line import *
import pandas as pd
from line import *

def control_files(access_token,dataset_,server_id,yy,batches_mt,thread_id,cpus_required,cpus):

    #print("\r\n" + "Generating control files...")
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
    #print("\r\n" + "Done generating control files...")



def mp_threads(access_token,dataset_,server_id,dataset_name,thread_id,dataset_currentVersionId,query_fields_str,q_limit,batch_count):

    time.sleep(0.01)

    if thread_id == 0:
        q_offset = 0
    else:
        q_offset = 50000 * thread_id

    saql = "q = load \"{}/{}\";q = foreach q generate {};q = offset q {};q = limit q {};".format(dataset_,dataset_currentVersionId,query_fields_str,q_offset,q_limit)
    saql_payload = {"name": "get_rows","query": str(saql), "queryLanguage": "SAQL"}
    saql_payload = json.dumps(saql_payload)

    headers = {'Authorization': "Bearer {}".format(access_token),
               'Content-Type': "application/json"
               }
    x = 0
    xx = 0
    while x != 1:
        #print(thread_id)
        resp = requests.post('https://{}.salesforce.com/services/data/v53.0/wave/query'.format(server_id), headers=headers, data=saql_payload)
        resp_text = json.loads(resp.text)
        try:
            query_results = ((resp_text.get("results").get("records")))
            query_results_json = json.dumps(query_results)

            #with open('{}_{}_query_results{}.json'.format(dataset_name,thread_id,batch_count), 'w') as outfile:
            #    json.dump(query_results, outfile)

            #outfile.close()

            #df = pd.read_json(r'{}_{}_query_results{}.json'.format(dataset_name,thread_id,batch_count))
            df = pd.read_json(query_results_json)
            df.fillna(0)
            export_csv = df.to_csv(r'{}_{}_query_results.csv'.format(dataset_name,thread_id), index = None, header=True, encoding='utf-8')

            #time.sleep(0.1)
            del export_csv
            gc.collect()
            x = 1
        except:
            xx += 1
            #prRed("\r\n" + "Error in process #{}. Trying again... {}".format(thread_id,xx) + "\r\n")
            time.sleep(5)
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

    #try:
    #    if os.path.exists('{}_{}_query_results{}.json'.format(dataset_name,thread_id,batch_count)):
    #        os.remove('{}_{}_query_results{}.json'.format(dataset_name,thread_id,batch_count))
    #except:
    #    pass

    #print("success")
