import json, requests, math, csv, glob, os, base64, threading, time, configparser, gc
from terminal_colors import *
from sfdc_login import *
import pandas as pd
from dataset_tasks.dataset_extract_MP import *
from dataset_tasks.MP_control import *
from line import *

def mp_to_mt(access_token,dataset_,server_id,dataset_name,dataset_currentVersionId,query_fields_str,q_limit,i,max_t_count,cpus_required):

    if os.path.exists("mp{}.ini".format(i)):
        config = configparser.ConfigParser()
        config.read("mp{}.ini".format(i))
        thread_id = int(config.get("DEFAULT", "thread_id"))
        batches_mt = int(config.get("DEFAULT", "batches_mt"))
        last = (config.get("DEFAULT", "last"))
        batches_mt = int(batches_mt)
        thread_id = int(thread_id)
        file_id = int(thread_id)
        file_max = int(batches_mt)
        progress = 0
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
                progress += index / batches_mt
                #print(progress)
                config = configparser.ConfigParser()
                with open("p{}.ini".format(i), 'w') as configfile:
                    config['DEFAULT'] = {'progress': '{}'.format(progress)}
                    config.write(configfile)
                    configfile.close()
                thread.join()
                time.sleep(0.1)




        if batches_mt > 9 and batches_mt < 100:
            batches_10 = math.ceil(batches_mt / max_t_count)
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
                job_count = max_t_count

                if job_count <= rem_jobs:
                    rem_jobs = rem_jobs - max_t_count
                    t_count = max_t_count
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
                    progress += len(threads) / batches_mt
                    #print(i,t_count,batches_mt,progress)
                    #print("\r\n")
                    config = configparser.ConfigParser()
                    with open("p{}.ini".format(i), 'w') as configfile:
                        config['DEFAULT'] = {'progress': '{}'.format(progress)}
                        config.write(configfile)
                        configfile.close()
                    time.sleep(0.1)

        if batches_mt > 100:
            batches_10 = math.ceil(batches_mt / max_t_count)
            print(batches_10)
            batch_count = 0
            batch_10_count = 0
            ii = 1
            rem_jobs = batches_mt
            job_count = 0
            #prCyan("\r\n" + "Starting extraction now... " + "\r\n")
            time.sleep(0.1)
            #prCyan("\r\n" + "Progress: " + "\r\n")


            while batch_10_count <= batches_10:
                batch_10_count += 1
                job_count = max_t_count

                if job_count <= rem_jobs:
                    rem_jobs = rem_jobs - max_t_count
                    t_count = max_t_count
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
                    time.sleep(0.1)

                for index, thread in enumerate(threads):
                    thread.join()
                    progress += len(threads) / batches_mt
                    #print(i,t_count,batches_mt,progress)
                    #print("\r\n")
                    config = configparser.ConfigParser()
                    with open("p{}.ini".format(i), 'w') as configfile:
                        config['DEFAULT'] = {'progress': '{}'.format(progress)}
                        config.write(configfile)
                        configfile.close()
                    time.sleep(0.1)

        csv_files = []

        time.sleep(1)

        if last == 'N' and file_id == 0 and cpus_required == 1:
            x = file_id
            file_max = batches_mt + file_id - 1
        elif last == 'N' and file_id == 0:
            x = file_id
            file_max = batches_mt + file_id
        elif last == 'N' and file_id != 0:
            x = file_id + 1
            file_max = batches_mt + file_id
        else:
            x = file_id + 1
            file_max = batches_mt + file_id - 1

        try:
            while x <= file_max:
                if x >= file_id and x <= file_max:
                    csv_files.append('{}_{}_query_results.csv'.format(dataset_name,x))
                x += 1
            #print("\r\n\r\n\r\n\r\n")
            #print('Process: ',i)
            #print(x,file_id,file_max)
            #print(csv_files)
            #print("\r\n\r\n\r\n\r\n")
            #time.sleep(1)
        except:
            pass


        try:
            combined_csv = pd.concat([pd.read_csv(csv_file, low_memory=False) for csv_file in csv_files])
            combined_csv.to_csv( "{}_dataset_extraction_split{}.csv".format(dataset_name,i), index=False, header=True, encoding='utf-8')
        except:
            pass

        del combined_csv
        del csv_files
        gc.collect()

    return i
