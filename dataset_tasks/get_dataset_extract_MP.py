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
import time
import sys
import subprocess
from line import *
from dataset_tasks.MP_control import *
from dataset_tasks.mt_for_mp import *
import gc
import psutil
from b2h import *

def delete_last():
    sys.stdout.write('\x1b[1A')
    sys.stdout.write('\x1b[2K')

def get_datasets_extract_mp(access_token,dataset_,server_id):

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

    try:
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
    except:
        pass

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

    except ValueError:
        prRed("there are no dimensions present in the dataset.")

    #def convert_list_to_string(query_fields, seperator=','):
    #    return seperator.join(query_fields)

    #query_fields_str = convert_list_to_string(query_fields)


    query_fields_str = ', '.join(f'\'{w}\'' for w in query_fields)

    total_fields = dimension_counter + measures_counter
    #print(total_fields)

    i = 0
    q_limit = 9999
    q_offset = 0
    dataset_extraction_dir = "dataset_extraction"

    #print("start")
    #multicore & threads function to submit the queries
    if batches_ > 0:
        pool = mp.Pool((mp.cpu_count()))
        cpus = int(mp.cpu_count())
        prCyan("\r\n" + "Starting extraction using all {} CPU Cores...".format(cpus) + "\r\n" + "\r\n" + "\r\n")
        line_print()
        prCyan("\r\n")
        mts = math.ceil(batches_ / cpus)
        pool_cycles_A = math.ceil(batches_ / cpus)
        pool_cycles_B = math.floor(batches_ / cpus)
        control_flag = mts
        control = round(batches_ / 2)
        thread_count = 0
        thread_id = 0
        yy = 1
        remain_jobs = batches_
        cpu_control = 0
        cpus_required = 0

        while cpu_control < batches_:
            cpu_control += pool_cycles_A + 1
            cpus_required += 1

        for zz in range(cpus_required):
            #print("zz {}".format(zz))
            #print("cpus_required {}".format(cpus_required))
            if thread_count < batches_ and (zz + 1) < cpus_required:
                batches_mt = pool_cycles_A + 1
                thread_count += batches_mt
            else:
                batches_mt = batches_ - thread_count

            control_files(access_token,dataset_,server_id,zz,batches_mt,thread_id)

            thread_id += batches_mt

            remain_jobs = batches_ - batches_mt

            sum_control = batches_mt + thread_count

        _start = time.time()

        #prRed("Calling MP Function now.")
        #result_async = [pool.apply_async(data_extract_mp, args = (dataset_,dataset_currentVersionId,query_fields_str,q_offset,q_limit,i,access_token,dataset_name,server_id,batches_,query_fields, )) for i in
                        #range(batches_)]

        result_async = [pool.apply_async(mp_to_mt, args = (access_token,dataset_,server_id,dataset_name,dataset_currentVersionId,query_fields_str,q_limit,i, )) for i in
                        range(cpus_required)]

        if batches_ >= cpus_required:
            if cpus_required < cpus:
                s_time = 0.1
            else:
                s_time = 0.5
            try:
                yyy = 0
                xxx = 0
                zzz = 0
                for r in result_async:
                    while yyy <= batches_:
                        xxx = 0
                        zzz = 0
                        for xxx in range(cpus_required):
                            #print("xxx for {}".format(xxx))
                            #time.sleep(2)
                            try:

                                if os.path.exists("p{}.ini".format(xxx)):
                                    #print(xxx)
                                    #print("p{}.ini".format(xxx))
                                    config = configparser.ConfigParser()
                                    config.read("p{}.ini".format(xxx))

                                    try:
                                        #print((config.get("DEFAULT", "progress{}".format(zzz))))
                                        yyy += int(round(int(config.get("DEFAULT", "progress")) / 1))
                                    except:
                                        yyy += cpus_required

                                    progress = round((yyy / batches_) * 100,1)

                                    #zzz += 1

                                    #print("yyy {}".format(yyy) + "progress {}".format(progress) + "\r\n" + "\r\n"+ "\r\n")
                                    if progress < 10:
                                        iostat1 = psutil.net_io_counters(pernic=False)
                                        iostat1 = int(iostat1[1])
                                        time.sleep(s_time)
                                        delete_last()
                                        delete_last()
                                        delete_last()
                                        prGreen("Progress:\r")
                                        prYellow("  {}%\r".format(progress))
                                        iostat2 = psutil.net_io_counters(pernic=False)
                                        iostat2 = int(iostat2[1])
                                        speed_dn = iostat2 - iostat1
                                        speed_dn = bytes2human(speed_dn)
                                        print("Download Speed: {}/s".format(speed_dn))
                                    elif progress < 30:
                                        iostat1 = psutil.net_io_counters(pernic=False)
                                        iostat1 = int(iostat1[1])
                                        time.sleep(s_time)
                                        delete_last()
                                        delete_last()
                                        delete_last()
                                        prGreen("Progress:\r")
                                        prYellow(" {}%\r".format(progress))
                                        iostat2 = psutil.net_io_counters(pernic=False)
                                        iostat2 = int(iostat2[1])
                                        speed_dn = iostat2 - iostat1
                                        speed_dn = bytes2human(speed_dn)
                                        print("Download Speed: {}/s".format(speed_dn))
                                    elif progress < 60:
                                        iostat1 = psutil.net_io_counters(pernic=False)
                                        iostat1 = int(iostat1[1])
                                        time.sleep(s_time)
                                        delete_last()
                                        delete_last()
                                        delete_last()
                                        prGreen("Progress:\r")
                                        prLightPurple(" {}%\r".format(progress))
                                        iostat2 = psutil.net_io_counters(pernic=False)
                                        iostat2 = int(iostat2[1])
                                        speed_dn = iostat2 - iostat1
                                        speed_dn = bytes2human(speed_dn)
                                        print("Download Speed: {}/s".format(speed_dn))
                                    elif progress < 100:
                                        iostat1 = psutil.net_io_counters(pernic=False)
                                        iostat1 = int(iostat1[1])
                                        time.sleep(s_time)
                                        delete_last()
                                        delete_last()
                                        delete_last()
                                        prGreen("Progress:\r")
                                        prCyan(" {}%\r".format(progress))
                                        iostat2 = psutil.net_io_counters(pernic=False)
                                        iostat2 = int(iostat2[1])
                                        speed_dn = iostat2 - iostat1
                                        speed_dn = bytes2human(speed_dn)
                                        print("Download Speed: {}/s".format(speed_dn))
                                    time.sleep(0.25)
                                #print("xxx {}".format(xxx))
                                #print("yyy {}".format(yyy))
                                #print("batches_ {}".format(batches_))
                                #print("\r\n")
                            except:
                                #xxx -= 1
                                pass
            except:
                pass

        #results = [r.get() for r in result_async]

        pool.close()
        pool.join()
        delete_last()
        delete_last()
        prGreen(" 100%\r")
        line_print()

    del_ = 0
    for del_ in range(cpus_required):
        if os.path.exists('mp{}.ini'.format(del_)):
            os.remove('mp{}.ini'.format(del_))
        if os.path.exists('p{}.ini'.format(del_)):
            os.remove('p{}.ini'.format(del_))
            del_ += 1

    if batches_ > 0:
        #Folder check for existing files - start:
        if os.path.exists("{}_dataset_extraction.csv".format(dataset_name)):
            os.remove("{}_dataset_extraction.csv".format(dataset_name))

        #del_ = 0
        #for del_ in range(batches_):
        #    if os.path.exists('{}_{}_query_results.json'.format(dataset_name,del_)):
        #        os.remove('{}_{}_query_results.json'.format(dataset_name,del_))
        #        del_ += 1

        #if os.path.exists('{}_{}_query_results.csv'.format(dataset_name,i)):
        #    os.remove('{}_{}_query_results.csv'.format(dataset_name,i))
        #Folder check for existing files - end.

        #Append all csv files from the batches - start:

        prGreen("\r\n" + "Compiling CSV." + "\r\n")
        extension = 'csv'
        csv_start = time.time()
        csv_files = glob.glob('{}_*.{}'.format(dataset_name,extension))
        #print(csv_files)
        combined_csv = pd.concat([pd.read_csv(csv_file) for csv_file in csv_files])
        combined_csv.fillna(0)
        combined_csv.to_csv( "{}_dataset_extraction.csv".format(dataset_name), index=False, header=True, encoding='utf-8')

        #cmd = "Get-ChildItem -Filter *_results.csv | Select-Object -ExpandProperty FullName | Import-Csv | Export-Csv .\{}_extract.csv -NoTypeInformation".format(dataset_name)
        #completed = subprocess.run(["powershell", "-Command", cmd], capture_output=True)

        csv_end = time.time()
        total_csv = round((csv_end - csv_start),2)

        prCyan("\r\n" + "Dataset Succesfully Exported in {}".format(total_csv) + "\r\n")
        line_print()
        prGreen("\r\n" + "Data sample:" + "\r\n")
        print(combined_csv)
        line_print()
        prCyan("\r\n" + "Find the file here: {}".format(d_ext) + "\r\n")
        time.sleep(0.2)
        del combined_csv
        gc.collect()

        #Append all csv files from the batches - end.

    _end = time.time()
    total_time = round((_end - _start),2)
    prGreen("\r\n" + "Extraction completed in {}s.".format(total_time))
    time.sleep(0.2)


    #Folder Cleanup
    del_ = 0
    for del_ in range(batches_):
        if os.path.exists('{}_{}_query_results.csv'.format(dataset_name,del_)):
            os.remove('{}_{}_query_results.csv'.format(dataset_name,del_))
            del_ += 1




    #Go back to parent folder:
    os.chdir("..")
