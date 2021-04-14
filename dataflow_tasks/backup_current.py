import json
import requests
from terminal_colors import *
from sfdc_login import *
import os
import time

def backup_dataflow_current(access_token,dataflow_his_url,dataflow_id_,dataflow_name_,server_id):

    #os.chdir("..")

    try:
        dataflow_extraction_dir = "dataflow_backup"
        os.mkdir(dataflow_extraction_dir)
    except OSError as error:
            print(" ")

    cd = os.getcwd()

    d_ext = "{}".format(cd)+"/dataflow_backup/"

    os.chdir(d_ext)

    previewUrl = "kusdyu232dusgx"

    headers = {
        'Authorization': "Bearer {}".format(access_token),
        'Content-Type': "application/json"
        }
    resp = requests.get('https://{}.salesforce.com{}'.format(server_id,dataflow_his_url), headers=headers)

    formatted_response = json.loads(resp.text)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    #prGreen(formatted_response_str)
    dataflow_his_list = formatted_response.get('histories')

    #Check if there are available histories to backup - start:

    counter = 0

    for x in dataflow_his_list:
        counter += 1

    #Check if there are available histories to backup - end.

    if counter != 0:
        counter = 0
        for x in dataflow_his_list:
            if counter == 0:
                dataflow_his_id_ = x["id"]
                historyUrl = x["historyUrl"]
                previewUrl = x["previewUrl"]
                privatePreviewUrl = x["privatePreviewUrl"]


        resp = requests.get('https://{}.salesforce.com{}'.format(server_id,previewUrl), headers=headers)

        formatted_response = json.loads(resp.text)
        formatted_response_str = json.dumps(formatted_response, indent=2)
        dataflow_his_list = formatted_response.get('definition')

        with open('{}_dataflow_backup.json'.format(dataflow_name_), 'w') as outfile:
            json.dump(dataflow_his_list, outfile)

        prYellow("\r\n" + "Dataflow JSON definition succesfully backed up here: {}".format(d_ext) + "\r\n")

        os.chdir("..")
    else:
        prRed("\r\n" + "There is no JSON available to backup." + "\r\n")
        os.chdir("..")
