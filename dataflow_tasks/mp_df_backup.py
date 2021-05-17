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


def mp_df_backup(access_token,server_id,i,dataflow_list,headers):

    x = i
    dataflow_his_list = dataflow_list[x]

    #print("{} - ".format(i) ,"Dataflow id: ",x["id"]," - Label: ",x["label"])
    dataflow_id_ = dataflow_his_list.get('id')
    dataflow_name_full = dataflow_his_list.get('name')
    dataflow_his_url = dataflow_his_list.get('historiesUrl')

    try:
        dataflow_name_ = remove(dataflow_name_full)
        #print(dataflow_name_)
    except:
        pass

    try:
        os.remove('{}_dataflow_backup.json'.format(dataflow_name_))
    except:
        pass

    headers = {
        'Authorization': "Bearer {}".format(access_token),
        'Content-Type': "application/json"
        }
    resp = requests.get('https://{}.salesforce.com{}'.format(server_id,dataflow_his_url), headers=headers)

    formatted_response = json.loads(resp.text)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    #prGreen(formatted_response_str)
    dataflow_his_list = formatted_response.get('histories')

    #print(dataflow_his_list)

    #Check if there are available histories to backup - start:

    counter = 0

    for x in dataflow_his_list:
        counter += 1

    #Check if there are available histories to backup - end.

    if counter != 0:

        dataflow_his_id_ = dataflow_his_list[0]["id"]
        #print(dataflow_his_id_)
        historyUrl = dataflow_his_list[0]["historyUrl"]
        #print(historyUrl)
        previewUrl = dataflow_his_list[0]["previewUrl"]
        #print(previewUrl)
        privatePreviewUrl = dataflow_his_list[0]["privatePreviewUrl"]

        resp = requests.get('https://{}.salesforce.com{}'.format(server_id,previewUrl), headers=headers)

        #formatted_response = html.unescape(resp.text)
        formatted_response = json.loads(resp.text)
        formatted_response_str = json.dumps(formatted_response, indent=2)
        dataflow_ = formatted_response.get('definition')
        #print(formatted_response)

        #for x in dataflow_:
        #    value = dataflow_[x]
            #print('{} {}'.format(x,value))

        with open('{}_dataflow_backup.json'.format(dataflow_name_), 'w') as outfile:
            json.dump(dataflow_, outfile)

        #quit()
        os_running = get_platform()

        if os_running == "Windows":

            a_ = "(Get-Content " + '{}_dataflow_backup.json'.format(dataflow_name_) + ").Replace('&quot;','\\\"') | Set-Content " + '{}_dataflow_backup.json'.format(dataflow_name_)
            b_ = "(Get-Content " + '{}_dataflow_backup.json'.format(dataflow_name_) + ").Replace('&#39;','''') | Set-Content " + '{}_dataflow_backup.json'.format(dataflow_name_)
            c_ = "(Get-Content " + '{}_dataflow_backup.json'.format(dataflow_name_) + ").Replace('&gt;','>') | Set-Content " + '{}_dataflow_backup.json'.format(dataflow_name_)
            d_ = "(Get-Content " + '{}_dataflow_backup.json'.format(dataflow_name_) + ").Replace('&lt;','<') | Set-Content " + '{}_dataflow_backup.json'.format(dataflow_name_)
            e_ = "(Get-Content " + '{}_dataflow_backup.json'.format(dataflow_name_) + ").Replace('&amp;','&') | Set-Content " + '{}_dataflow_backup.json'.format(dataflow_name_)
            ps_1_dict = {"a": "{}".format(a_), "b": "{}".format(b_), "c": "{}".format(c_), "d": "{}".format(d_), "e": "{}".format(e_)}

            for x in ps_1_dict.values():
                #print(x)
                completed = subprocess.run(["powershell", "-Command", x], capture_output=True)
                time.sleep(0.15)

        print("\r\n" + "Done backing up:")
        prLightPurple("{} ".format(dataflow_name_full))

        #time.sleep(0.15)

        #prYellow("\r\n" + "Dataflow selected: {} - {}".format(dataflow_name_, dataflow_id_) + "\r\n")


    else:
        prYellow("\r\n" + "Warning:")
        prRed("There is no JSON available to backup for: {}".format(dataflow_name_full))
        #time.sleep(0.15)
        #prYellow("\r\n" + "Dataflow selected: {} - {}".format(dataflow_name_, dataflow_id_) + "\r\n")
