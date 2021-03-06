import json, requests, os
from misc_tasks.terminal_colors import *
from misc_tasks.sfdc_login import *

def backup_xmd_system(access_token,dataset_):

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
    resp = requests.get('https://na156.salesforce.com/services/data/v51.0/wave/datasets/{}'.format(dataset_), headers=headers)

    formatted_response = json.loads(resp.text)
    #print(formatted_response)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    #prGreen(formatted_response_str)

    dataset_current_version_url = formatted_response.get('currentVersionUrl')
    dataset_currentVersionId = formatted_response.get('currentVersionId')
    dataset_currentNameId = formatted_response.get('name')

    dataset_current_version_url = "https://na156.salesforce.com" + "{}".format(dataset_current_version_url) + "/xmds/system"

    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }

    resp = requests.get('{}'.format(dataset_current_version_url), headers=headers)
    formatted_response = json.loads(resp.text)
    formatted_response_str = json.dumps(formatted_response, indent=2)

    with open('{}_backup_system.xmd.json'.format(dataset_currentNameId), 'w') as outfile:
        json.dump(formatted_response, outfile)

    print("\r\n" + "System XMD Succesfully Exported. Find the file here: {}".format(d_ext) + "\r\n")


    #Go back to parent folder:
    os.chdir("..")
