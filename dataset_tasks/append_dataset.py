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
from dataset_tasks.append_dataset_MT import *
import threading
import datetime
import sys
from line import *
import multiprocessing as mp
import psutil
from b2h import *

class Result():
    def __init__(self):
        self.val = 0

    def update_result(self, val):
        self.val += 1

def delete_last():
    sys.stdout.write('\x1b[1A')
    sys.stdout.write('\x1b[2K')

def append_csv_dataset(access_token,dataset_name_,dataset_,server_id,dataset_name):

    try:
        dataset_upload_dir = "dataset_upload"
        os.mkdir(dataset_upload_dir)
    except OSError as error:
            print(" ")

    cd = os.getcwd()
    #print(cd)

    d_ext = "{}".format(cd)+"\\dataset_upload\\"
    #print(d_ext)

    os.chdir(d_ext)

    user_input_1 = "Xhhrydjanshtttx"
    user_input_2 = "xbyr5546shdnc"
    user_input_3 = 9567385638567265

    #Input check for file placement
    while user_input_1 == "Xhhrydjanshtttx" or user_input_1 == "N" or user_input_1 == "n":
        user_input_1 = input("\r\n" + "Have you placed the CSV file in the \'dataset_upload\' folder? (Y/N): ")
        time.sleep(2)
        if user_input_1 == "Y" or user_input_1 == "y":
            print("\r\n")
        elif user_input_1 == "N" or user_input_1 == "n":
            prYellow("Please place the file and try again.")
            time.sleep(1)
        else:
            prRed("Wrong value. Try again.")
            time.sleep(1)

    #Input check for file encoding
    while user_input_2 == "xbyr5546shdnc" or user_input_2 == "N" or user_input_2 == "n":
        user_input_2 = input("\r\n" + "Is the CSV file comma separated and UTF-8 encoded? (Y/N): ")
        time.sleep(2)
        if user_input_2 == "Y" or user_input_2 == "y":
            print("")
            time.sleep(0.5)
        elif user_input_2 == "N" or user_input_2 == "n":
            prYellow("Please save your file as comma separated (not tab or semicolon) and ensure it's UTF-8 encoded.")
            time.sleep(2)
        else:
            prRed("Wrong value. Try again.")
            time.sleep(1)

    #Input check for total # of rows
    while user_input_3 == 9567385638567265 or type(user_input_3) != int or user_input_3 < 1:
        user_input_3 = input("\r\n" + "What's the total row count in your file? (integer): ")
        time.sleep(2)
        try:
            user_input_3 = int(user_input_3)
            if type(user_input_3) == int and user_input_3 > 0:
                print("")
                time.sleep(0.5)
            elif type(user_input_3) == int and user_input_3 < 1:
                prYellow("\r\n" + "Did you enter the right number of rows? Try again.")
                time.sleep(2)
            else:
                prRed("\r\n" + "Please use an integer.")
                time.sleep(1)
        except ValueError:
            prRed("\r\n" + "Please use an integer.")
            time.sleep(2)

    if (user_input_1 == "Y" or user_input_1 == "y") and (user_input_2 == "Y" or user_input_2 == "y"):
        dataset_name = input("\r\n" + "Enter your filename without the csv extension:")
        line_print()
        time.sleep(2)
        prGreen("\r\n" + "Locally generating json metadata from the csv file and encoding it to base64.")
        time.sleep(0.5)
        _start = time.time()
        csv_upload_json_meta(dataset_name_,dataset_name)
        meta_json_data = open("{}_CSV_upload_metadata.json".format(dataset_name), 'rb').read()
        meta_json_base64_encoded = base64.b64encode(meta_json_data).decode('UTF-8')
        #os.remove("{}_CSV_upload_metadata.json".format(dataset_name))
        _end = time.time()
        enc_time = round((_end-_start),2)
        prGreen("\r\n" + "Task Finished in {}s".format(enc_time))
        line_print()
        time.sleep(0.5)

        batches_ = math.ceil(user_input_3 / 55000)

        batch_count = 0

        skiprows = 0

        operation_flag = 'Append'

        if batches_ > 0:

            full_start = time.time()

            prGreen("\r\n" + "Your file will be upladed in {} batches".format(batches_))
            line_print()

            headers = {'Authorization': "Bearer {}".format(access_token),
                       'Content-Type': "application/json"}

            payload = {'Format' : 'Csv','EdgemartAlias' : '{}'.format(dataset_name_),'Operation': '{}'.format(operation_flag),'Action': 'None','MetadataJson': "{}".format(meta_json_base64_encoded)}
            payload = json.dumps(payload)
            prGreen("\r\n" + "Creating Workbench Job")

            x = 0
            xx = 5
            while x != 1:
                try:
                    resp = requests.post('https://{}.salesforce.com/services/data/v51.0/sobjects/InsightsExternalData'.format(server_id), headers=headers, data=payload)
                    time.sleep(0.5)
                    resp_results = json.loads(resp.text)
                    formatted_response_str = json.dumps(resp_results, indent=2)
                    #prYellow(formatted_response_str)
                    try:
                        success = resp_results.get('success')
                    except:
                        pass
                    try:
                        errors = resp_results.get('errors')
                    except:
                        pass
                    try:
                        job_id = resp_results.get("id")
                    except:
                        pass
                    try:
                        message = resp_results[0]["message"]
                    except:
                        pass
                    if job_id:
                        prGreen("\r\n" + "Workbench Job Id: {}".format(job_id))
                    else:
                        pass
                    if success:
                        prYellow("Status: Successful")
                        line_print()
                        x += 1
                        p_flag = 1
                    else:
                        try:
                            prRed(errors)
                            x += 1
                            batches_ = 0
                            p_flag = 0
                            time.sleep(0.5)
                            prRed("\r\n" + "Cancelling Job..." + "\r\n")
                        except:
                            prRed(message)
                            x += 1
                            batches_ = 0
                            p_flag = 0
                            prRed("\r\n" + "Cancelling Job..." + "\r\n")
                        time.sleep(0.5)
                except:
                    prRed(message)
                    x += 1
                    prRed("\r\n" + "Cancelling Job..." + "\r\n")
                    time.sleep(2)
                    batches_ = 0
                    p_flag = 0

            #i = 1
            #batches_10 = math.ceil(batches_ / 10)
            #batch_10_count = 0
            #delete_count = 0
            #ii = 1
            #rem_jobs = batches_
            #job_count = 0

            if p_flag == 1:
                pool = mp.Pool((mp.cpu_count()))
                cpus = int(mp.cpu_count())
                prCyan("\r\n" + "Starting upload using all {} CPU Cores...".format(cpus) + "\r\n")
                line_print()
                prCyan("\r\n")
                prGreen("Progress:\r")
                prYellow("  0.0%\r")
                mts = math.ceil(batches_ / cpus)
                pool_cycles_A = math.ceil(batches_ / cpus)
                pool_cycles_B = math.floor(batches_ / cpus)
                control_flag = mts
                control = round(batches_ / 2)
                thread_count = 0
                thread_id = 0
                progress = 0
                ret = 0
                ind = 0
                yyy = 0
                result = Result()
                #print(batches_)

                result_async = [pool.apply_async(data_append_mp, args = (dataset_name,skiprows,job_id,server_id,access_token,i, ), callback=result.update_result) for i in range(batches_)]

                if batches_ >= 1:
                    try:
                        while yyy <= batches_:
                            xxx = 0
                            zzz = 0
                            #print(result.val)
                            #print("\r\n" + "\r\n" + "\r\n")
                            for xxx in range(cpus):
                                #print(ind)
                                yyy += ( result.val / batches_ ) / cpus
                                progress = round((yyy / batches_) * 100,1)
                                if progress < 10:
                                    iostat1 = psutil.net_io_counters(pernic=False)
                                    iostat1 = int(iostat1[0])
                                    time.sleep(1)
                                    delete_last()
                                    delete_last()
                                    delete_last()
                                    prGreen("Progress:\r")
                                    prYellow("  {}%\r".format(progress))
                                    iostat2 = psutil.net_io_counters(pernic=False)
                                    iostat2 = int(iostat2[0])
                                    speed_dn = iostat2 - iostat1
                                    speed_dn = bytes2human(speed_dn)
                                    print("Upload Speed: {}/s".format(speed_dn))
                                elif progress < 30:
                                    iostat1 = psutil.net_io_counters(pernic=False)
                                    iostat1 = int(iostat1[0])
                                    time.sleep(1)
                                    delete_last()
                                    delete_last()
                                    delete_last()
                                    prGreen("Progress:\r")
                                    prYellow(" {}%\r".format(progress))
                                    iostat2 = psutil.net_io_counters(pernic=False)
                                    iostat2 = int(iostat2[0])
                                    speed_dn = iostat2 - iostat1
                                    speed_dn = bytes2human(speed_dn)
                                    print("Upload Speed: {}/s".format(speed_dn))
                                elif progress < 60:
                                    iostat1 = psutil.net_io_counters(pernic=False)
                                    iostat1 = int(iostat1[0])
                                    time.sleep(1)
                                    delete_last()
                                    delete_last()
                                    delete_last()
                                    prGreen("Progress:\r")
                                    prLightPurple(" {}%\r".format(progress))
                                    iostat2 = psutil.net_io_counters(pernic=False)
                                    iostat2 = int(iostat2[0])
                                    speed_dn = iostat2 - iostat1
                                    speed_dn = bytes2human(speed_dn)
                                    print("Upload Speed: {}/s".format(speed_dn))
                                elif progress < 100:
                                    iostat1 = psutil.net_io_counters(pernic=False)
                                    iostat1 = int(iostat1[0])
                                    time.sleep(1)
                                    delete_last()
                                    delete_last()
                                    delete_last()
                                    prGreen("Progress:\r")
                                    prCyan(" {}%\r".format(progress))
                                    iostat2 = psutil.net_io_counters(pernic=False)
                                    iostat2 = int(iostat2[0])
                                    speed_dn = iostat2 - iostat1
                                    speed_dn = bytes2human(speed_dn)
                                    print("Upload Speed: {}/s".format(speed_dn))
                                time.sleep(0.25)
                    except:
                        pass

                    pool.close()
                    pool.join()
                    delete_last()
                    delete_last()
                    prGreen(" 100%\r")
                    line_print()


                    payload = {'Action' : 'Process'}
                    payload = json.dumps(payload)

                    x = 0
                    while x != 1:
                        try:
                            resp = requests.patch('https://{}.salesforce.com/services/data/v51.0/sobjects/InsightsExternalData/{}'.format(server_id,job_id), headers=headers, data=payload)
                            prGreen("\r\n" + "All {} batches uploaded.".format(batches_))
                            prYellow("TCRM Data Manager Job triggered. Check the data manager for more details." + "\r\n")
                            x += 1
                            time.sleep(1)
                        except:
                            pass

                    full_end = time.time()
                    full_time = round((full_end-full_start),2)
                    full_time = time.strftime("%H h : %M m : %S s", time.gmtime(full_time))
                    prGreen("\r\n" + "Append Completed in {}".format(full_time))
                    line_print()

    #Go back to parent folder:
    os.chdir("..")
    prCyan("\r\n" + "Dataset selected: {} - {}".format(dataset_name, dataset_))
