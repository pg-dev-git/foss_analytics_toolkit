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
from dataset_tasks.dataset_extract_MP import *
import time
import configparser
from dataset_tasks.MP_control import *
from line import *

def mp_to_mt(access_token,dataset_,server_id,dataset_name,dataset_currentVersionId,query_fields_str,q_limit,i):

    if os.path.exists("mp{}.ini".format(i)):
        config = configparser.ConfigParser()
        config.read("mp{}.ini".format(i))
        thread_id = int(config.get("DEFAULT", "thread_id"))
        batches_mt = int(config.get("DEFAULT", "batches_mt"))
        batches_mt = int(batches_mt)
        thread_id = int(thread_id)
        #print("batches_mt start:{}".format(batches_mt))
        #print("thread_id start:{}".format(thread_id))

        if batches_mt > 0 and batches_mt <10:
            batch_count = 0

            threads = list()

            for index in range(batches_mt):
                #print(thread_id)
                x = threading.Thread(target=mp_threads, args=(access_token,dataset_,server_id,dataset_name,thread_id,dataset_currentVersionId,query_fields_str,q_limit,batch_count,))
                threads.append(x)
                x.start()
                thread_id += 1
                batch_count += 1
                time.sleep(0.25)

            #prCyan("\r\n" + "Progress: " + "\r\n")

            for index, thread in enumerate(threads):
                config = configparser.ConfigParser()
                with open("p{}.ini".format(i), 'w') as configfile:
                    config['DEFAULT'] = {'progress': '{}'.format(index)}
                    config.write(configfile)
                configfile.close()
                thread.join()
                time.sleep(0.01)




        if batches_mt > 9:
            batches_10 = math.ceil(batches_mt / 10)
            batch_count = 0
            batch_10_count = 0
            ii = 1
            rem_jobs = batches_mt
            job_count = 0
            #prCyan("\r\n" + "Starting extraction now... " + "\r\n")
            time.sleep(0.01)
            #prCyan("\r\n" + "Progress: " + "\r\n")
            while batch_10_count <= batches_10:
                batch_10_count += 1
                job_count = 10

                if job_count <= rem_jobs:
                    rem_jobs = rem_jobs - 10
                    t_count = 10
                else:
                    t_count = rem_jobs
                    rem_jobs = 0

                threads = list()
                #prCyan("\r\n" + "Starting {} CPU threads to extract the dataset".format(batches_) + "\r\n")
                _start = time.time()
                for index in range(t_count):
                    x = threading.Thread(target=mp_threads, args=(access_token,dataset_,server_id,dataset_name,thread_id,dataset_currentVersionId,query_fields_str,q_limit,batch_count,))
                    threads.append(x)
                    x.start()
                    batch_count += 1
                    thread_id += 1
                    time.sleep(0.01)

                for index, thread in enumerate(threads):
                    thread.join()
                    config = configparser.ConfigParser()
                    #print(index)
                    with open("p{}.ini".format(i), 'w') as configfile:
                        config['DEFAULT'] = {'progress': '{}'.format(index)}
                        config.write(configfile)
                    configfile.close()
                    time.sleep(0.01)


    return 0
