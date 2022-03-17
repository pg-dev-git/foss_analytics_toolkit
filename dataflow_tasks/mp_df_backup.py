import json, requests, time, sys, subprocess, datetime, shutil, os, traceback, html, re, multiprocessing as mp
from misc_tasks.terminal_colors import *
from misc_tasks.sfdc_login import *
from dashboards_tasks.get_dash_datasets import *
from misc_tasks.zipper import *
from misc_tasks.line import *

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

        formatted_response = json.loads(resp.text)
        formatted_response_str = json.dumps(formatted_response, indent=2)
        dataflow_ = formatted_response.get('definition')

        with open('{}_dataflow_backup.json'.format(dataflow_name_), 'w') as outfile:
            json.dump(dataflow_, outfile)

        try:
            json_bkp_unscapped = open('{}_dataflow_backup.json'.format(dataflow_name_), 'r')
            json_bkp_unscapped = (json_bkp_unscapped.read())
            json_bkp_unscapped = json.loads(json_bkp_unscapped)
            json_bkp_unscapped = json.dumps(json_bkp_unscapped)
            json_bkp_unscapped = json_bkp_unscapped.replace('&quot;','\&quot;')

            regexp = "&.+?;"
            _html = re.findall(regexp, json_bkp_unscapped)
            for e in _html:
                unescaped = html.unescape(e)
                json_bkp_unscapped = json_bkp_unscapped.replace(e, unescaped)

            with open('{}_dataflow_backup.json'.format(dataflow_name_), 'w') as outfile:
                json.dump(json.loads(json_bkp_unscapped), outfile)
        except:
            traceback.print_exc()

        prLightPurple("{} ".format(dataflow_name_full))


    else:
        prYellow("\r\n" + "Warning:")
        prRed("There is no JSON available to backup for: {}".format(dataflow_name_full))
