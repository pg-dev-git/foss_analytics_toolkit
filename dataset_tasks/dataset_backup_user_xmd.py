import json
import requests
from terminal_colors import *
from sfdc_login import *
import os
import time

def backup_xmd_user(access_token,dataset_,server_id):

    #Backup folder creation - start:
    try:
        dataset_xmd_dir = "xmd_backups"
        os.mkdir(dataset_xmd_dir)
    except OSError as error:
            print(" ")
    #Backup folder creation - end.

    cd = os.getcwd()
    #print(cd)

    d_ext = "{}".format(cd)+"/xmd_backups/"
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
    dataset_currentNameId = formatted_response.get('name')

    dataset_current_version_url = "https://{}.salesforce.com".format(server_id) + "{}".format(dataset_current_version_url) + "/xmds/user"

    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }

    resp = requests.get('{}'.format(dataset_current_version_url), headers=headers)
    formatted_response = json.loads(resp.text)
    formatted_response_str = json.dumps(formatted_response, indent=2)

    with open('{}_backup_user.xmd.json'.format(dataset_currentNameId), 'w') as outfile:
        json.dump(formatted_response, outfile)

    prGreen("\r\n" + "User XMD Succesfully Exported. Find the file here: {}".format(d_ext) + "\r\n")

    time.sleep(2)


    #Go back to parent folder:
    os.chdir("..")
