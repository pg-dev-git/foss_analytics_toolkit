import json
import requests
from terminal_colors import *
from sfdc_login import *
import os
from dataflow_tasks.start_stop_dataflow import *
from dataflow_tasks.get_dataflow_history import *
from dataflow_tasks.mp_df_backup import *
from line import *
import datetime
from zipper import *
import shutil
import multiprocessing as mp


def remove(string):
    return string.replace(" ", "_")

def mass_dataflows(access_token,server_id):

    now = datetime.datetime.now()

    dt_string = now.strftime("%d-%m-%Y_%H_%M")

    #print(dt_string)

    try:
        dataflow_extraction_dir = "dataflow_backup"
        os.mkdir(dataflow_extraction_dir)
    except OSError as error:
            pass

    cd = os.getcwd()

    d_ext = "{}".format(cd)+"\\dataflow_backup\\"

    os.chdir(d_ext)

    try:
        dataflow_extraction_dir = 'mass_dataflows_backup_{}'.format(dt_string)
        os.mkdir(dataflow_extraction_dir)
    except OSError as error:
            pass

    cd = os.getcwd()

    d_ext = "{}".format(cd)+"\\{}\\".format(dataflow_extraction_dir)

    os.chdir(d_ext)

    #print(d_ext)

    prGreen("\r\n" + "Getting Dataflows List..." + "\r\n")
    line_print()

    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }
    resp = requests.get('https://{}.salesforce.com/services/data/v51.0/wave/dataflows'.format(server_id), headers=headers)
    #print(resp.json())

    #Print PrettyJSON in Terminal
    formatted_response = json.loads(resp.text)
    #print(formatted_response)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    #prGreen(formatted_response_str)

    dataflow_list = formatted_response.get('dataflows')

    counter = 0

    dataflow_id_ = 999999999

    for x in dataflow_list:
        counter += 1

    prGreen("\r\n" + "{} Dataflows will be backed up now...".format(counter) + "\r\n")
    line_print()

    _start = time.time()

    i = 0

    if counter != 0:
        
        pool = mp.Pool((mp.cpu_count()))
        cpus = int(mp.cpu_count())
        pool_cycles_A = math.ceil(counter / cpus)
        cpu_control = 0
        cpus_required = 0

        while cpu_control < counter:
            cpu_control += pool_cycles_A + 1
            cpus_required += 1

        prCyan("\r\n" + "Starting backup using all {} CPU Cores...".format(cpus) + "\r\n" + "\r\n")
        line_print()

        result_async = [pool.apply_async(mp_df_backup, args = (access_token,server_id,i,dataflow_list,headers, )) for i in
                        range(counter)]

        pool.close()
        pool.join()
        line_print()

    else:
        prRed("\r\n" + "There aren't Dashboards available to backup." + "\r\n")
        line_print()
        time.sleep(0.15)




    _end = time.time()

    total_time = round((_end - _start),2)

    os.chdir("..")
    directory = './{}'.format(dataflow_extraction_dir)
    tcrm_zipper(directory,dataflow_extraction_dir)
    shutil.rmtree(r'./{}'.format(dataflow_extraction_dir))
    prGreen("\r\n" + "Mass Dataflow Backup succesfully completed in {}s.".format(total_time))
    time.sleep(0.15)
    cd = os.getcwd()
    prCyan("\r\n" + "Find the files here: ")
    prLightPurple("\r\n" + "{}".format(cd))
    #line_print()


    os.chdir("..")
