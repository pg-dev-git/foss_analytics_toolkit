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
from dataset_tasks.dataset_extract_MT import *
import time

#os.chdir("/Users/pgagliar/Desktop/api_test/")

def xmd_cleanup(access_token,dataset_,server_id):

    try:
        dataset_extraction_dir = "xmd_backups"
        os.mkdir(dataset_extraction_dir)
    except OSError as error:
            print(" ")

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
    dataset_name = formatted_response.get('name')

    dataset_current_version_url = "https://{}.salesforce.com".format(server_id) + "{}".format(dataset_current_version_url) + "/xmds/main"

    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }

    resp = requests.get('{}'.format(dataset_current_version_url), headers=headers)
    formatted_response = json.loads(resp.text)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    #prYellow(formatted_response_str)

#Cleanup Measures - Start:
    try:
        #prCyan("\r\n" + "Cleaning Measures")
        fields = formatted_response.get('measures')
        fields_counter = 0
        for x in fields:
            fields_counter += 1
            if x['label']:
                y = x['label']
                try:
                    y = y.replace('.',' ')
                except:
                    pass
                try:
                    y = y.replace('_',' ')
                except:
                    pass
                try:
                    y = y.replace('__c','')
                except:
                    pass
                #print(y)
                del x['label']
                x['label'] = y
        formatted_response.pop('measures')
        formatted_response['measures'] = fields
        #time.sleep(1)
    except:
        pass
#Cleanup Measures - End.

#Cleanup Dimensions - multivalue - Start:
    try:
        #prCyan("\r\n" + "Cleaning Dimensions")
        fields = formatted_response.get('dimensions')
        fields_counter = 0
        for x in fields:
            fields_counter += 1
            #print(type(x['isMultiValue']))
            if x['isMultiValue'] == True or x['isMultiValue'] == False or type(x['isMultiValue']) == bool:
                del x['isMultiValue']
                #print("deleting multivalue {}".format(fields_counter))
        formatted_response.pop('dimensions')
        formatted_response['dimensions'] = fields
        #time.sleep(0.5)
    except:
        pass
#Cleanup Dimensions - multivalue - End.

#Cleanup Dimensions - type - Start:
    try:
        #prCyan("\r\n" + "Cleaning Dimensions")
        fields = formatted_response.get('dimensions')
        fields_counter = 0
        for x in fields:
            fields_counter += 1
            if x['type']:
                del x['type']
                #print("deleting type {}".format(fields_counter))
        formatted_response.pop('dimensions')
        formatted_response['dimensions'] = fields
        #time.sleep(0.5)
    except:
        pass
#Cleanup Dimensions - type - End.

#Cleanup Dimensions - characters - Start:
    try:
        #prCyan("\r\n" + "Cleaning Dimensions")
        fields = formatted_response.get('dimensions')
        fields_counter = 0
        for x in fields:
            fields_counter += 1
            if x['label']:
                y = x['label']
                try:
                    y = y.replace('.',' ')
                except:
                    pass
                try:
                    y = y.replace('_',' ')
                except:
                    pass
                try:
                    y = y.replace('__c','')
                except:
                    pass
                #print(y)
                del x['label']
                x['label'] = y
        formatted_response.pop('dimensions')
        formatted_response['dimensions'] = fields
        #time.sleep(0.5)
    except:
        pass
#Cleanup Dimensions - characters - End.

#Cleanup Dates - Start:
    try:
        #prCyan("\r\n" + "Adjusting Dates")
        fields = formatted_response.get('dates')
        fields_counter = 0
        for x in fields:
            fields_counter += 1
            if x['alias']:
                y = x['alias']
                try:
                    y = y.replace('.',' ')
                except:
                    pass
                try:
                    y = y.replace('_',' ')
                except:
                    pass
                try:
                    y = y.replace('__c','')
                except:
                    pass
                #print(y)
                del x['alias']
                x['alias'] = y
        formatted_response.pop('dates')
        formatted_response['dates'] = fields
        #time.sleep(0.5)
    except:
        pass
#Cleanup Dates - End.

#Cleanup Dates - type - Start:
    try:
        #prCyan("\r\n" + "Cleaning Dimensions")
        fields = formatted_response.get('dates')
        fields_counter = 0
        for x in fields:
            fields_counter += 1
            if x['type']:
                del x['type']
                #print("deleting type {}".format(fields_counter))
        formatted_response.pop('dates')
        formatted_response['dates'] = fields
        #time.sleep(0.5)
    except:
        pass
#Cleanup Dates - type - End.

#Update type - Start:
    try:
        formatted_response.pop('type')
        formatted_response['type'] = "user"
    except:
        pass
#Update type - End.

#Update url - Start:
    try:
        put_url = formatted_response.get('url')
        put_url = put_url.replace('main','user')
        formatted_response.pop('url')
        formatted_response['url'] = put_url
    except:
        pass
#Update url - End.

#Cleanup Derived Measures - Start:
    try:
        deriv_meas_format = formatted_response.get('derivedMeasures')
        for x in deriv_meas_format:
            fields_counter += 1
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

    formatted_response.pop('createdBy')
    formatted_response.pop('url')
    formatted_response.pop('type')
    formatted_response.pop('lastModifiedDate')
    formatted_response.pop('lastModifiedBy')
    formatted_response.pop('language')
    formatted_response.pop('createdDate')
    formatted_response.pop('dataset')

    with open('{}_clean_user.xmd.json'.format(dataset_), 'w') as outfile:
        json.dump(formatted_response, outfile)
    prGreen("\r\n" + "XMD Succesfully Cleaned Up.")
    prGreen("\r\n" + "You can find it here: {}".format(d_ext))

    time.sleep(2)

    user_input = input("\r\n" + "Do you want to push this new XMD to TCRM now? \"Y\" to confirm or hit any other key to cancel: ")

    if user_input == "Y" or user_input == "y":

        prCyan("\r\n" + "Updating TCRM XMD")
        headers = {'Authorization': "Bearer {}".format(access_token),
                   'Content-Type': "application/json"}
        payload = {}
        payload['dates'] = formatted_response.get('dates')
        payload['measures'] = formatted_response.get('measures')
        payload['dimensions'] = formatted_response.get('dimensions')
        payload['derivedMeasures'] = formatted_response.get('derivedMeasures')
        payload['derivedDimensions'] = formatted_response.get('derivedDimensions')
        payload = json.dumps(payload)
        resp = requests.put('https://{}.salesforce.com'.format(server_id) + '{}'.format(put_url), headers=headers,data=payload)
        time.sleep(1)
        prCyan("\r\n" + "XMD Updated")
        time.sleep(1)

    #Go back to parent folder:
    os.chdir("..")
