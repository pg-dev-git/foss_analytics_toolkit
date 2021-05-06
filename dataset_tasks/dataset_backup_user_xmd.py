import json
import requests
from terminal_colors import *
from sfdc_login import *
import os
import time

def backup_xmd_user(access_token,dataset_,server_id,dataset_name):

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

    #Cleanup JSON - Start

    formatted_response.pop('createdBy')
    formatted_response.pop('url')
    formatted_response.pop('type')
    formatted_response.pop('lastModifiedDate')
    formatted_response.pop('lastModifiedBy')
    formatted_response.pop('language')
    formatted_response.pop('createdDate')
    formatted_response.pop('dataset')

    #Cleanup JSON - End

#Cleanup Derived Measures - Start:
    try:
        deriv_meas_format = formatted_response.get('derivedMeasures')
        for x in deriv_meas_format:
            #fields_counter += 1
            if x['format']:
                format_ = x['format']
                #format_ = format.get('customFormat')
                format_ = format_['customFormat']
                new_form = format_.replace('&quot;','\"')
                x['format']['customFormat'] = new_form
        formatted_response.pop('derivedMeasures')
        formatted_response['derivedMeasures'] = deriv_meas_format
    except:
        pass
#Cleanup Derived Measures - End.

    with open('{}_backup_user.xmd.json'.format(dataset_currentNameId), 'w') as outfile:
        json.dump(formatted_response, outfile)

    time.sleep(1)

    prGreen("\r\n" + "User XMD Succesfully Exported. Find the file here: ")

    time.sleep(0.5)

    prLightPurple("\r\n" + "{}".format(d_ext) + "\r\n")

    time.sleep(2)


    #Go back to parent folder:
    os.chdir("..")
    prCyan("\r\n" + "Dataset selected: {} - {}".format(dataset_name, dataset_))
