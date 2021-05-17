import json
import requests
from terminal_colors import *
from sfdc_login import *
from dashboards_tasks.get_dash_datasets import *
import time
import sys
import subprocess
from zipper import *
from line import *
import datetime
import shutil
import multiprocessing as mp
from mp_dash_backup import *
import os

def remove(string):
    return string.replace(" ", "_")

def get_platform():
    platforms = {
        'linux1' : 'Linux',
        'linux2' : 'Linux',
        'darwin' : 'OS X',
        'win32' : 'Windows'
    }
    if sys.platform not in platforms:
        return sys.platform

    return platforms[sys.platform]

def mass_dashboards(access_token,server_id):

    now = datetime.datetime.now()

    dt_string = now.strftime("%d-%m-%Y_%H_%M")

    try:
        dataflow_extraction_dir = "dashboard_backup"
        os.mkdir(dataflow_extraction_dir)
    except OSError as error:
            pass

    cd = os.getcwd()

    d_ext = "{}".format(cd)+"\\dashboard_backup\\"

    os.chdir(d_ext)

    try:
        dataflow_extraction_dir = 'mass_dashboard_backup_{}'.format(dt_string)
        os.mkdir(dataflow_extraction_dir)
    except OSError as error:
            pass

    cd = os.getcwd()

    d_ext = "{}".format(cd)+"\\{}\\".format(dataflow_extraction_dir)

    os.chdir(d_ext)

    prGreen("\r\n" + "Getting dashboards list..." + "\r\n")
    line_print()

    time.sleep(1)

    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }
    resp = requests.get('https://{}.salesforce.com/services/data/v51.0/wave/dashboards'.format(server_id), headers=headers)
    #print(resp.json())
    #Print PrettyJSON in Terminal

    formatted_response = json.loads(resp.text)
    #print(formatted_response)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    #prGreen(formatted_response_str)

    dashboards_list = formatted_response.get('dashboards')

    counter = 0

    for x in dashboards_list:
        counter += 1

    if counter == 1:
        prGreen("\r\n" + "{} Dashboard will be backed up now...".format(counter) + "\r\n")
    else:
        prGreen("\r\n" + "{} Dashboards will be backed up now...".format(counter) + "\r\n")

    line_print()

    _start = time.time()

    i = 0

    #print(dashboards_list[i]["id"])


    if counter !=0:

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

        result_async = [pool.apply_async(mp_dash_backup, args = (access_token,server_id,i,dashboards_list,headers, )) for i in
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
    prGreen("\r\n" + "Mass Backup succesfully completed in {}s.".format(total_time))
    time.sleep(0.15)
    cd = os.getcwd()
    prCyan("\r\n" + "Find the files here: ")
    prLightPurple("\r\n" + "{}".format(cd))
    #line_print()


    os.chdir("..")
