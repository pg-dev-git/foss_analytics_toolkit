import json
import requests
from terminal_colors import *
from sfdc_login import *
import os
import time
import html
import sys
import subprocess

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

def backup_dataflow_current(access_token,dataflow_his_url,dataflow_id_,dataflow_name_,server_id):

    #os.chdir("..")

    try:
        dataflow_extraction_dir = "dataflow_backup"
        os.mkdir(dataflow_extraction_dir)
    except OSError as error:
            print(" ")

    cd = os.getcwd()

    d_ext = "{}".format(cd)+"\\dataflow_backup\\"

    os.chdir(d_ext)

    previewUrl = "kusdyu232dusgx"

    try:
        os.remove('{}_dataflow_backup.json'.format(dataflow_name_))
    except:
        pass

    print("----------------------------------------")
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
                #print(dataflow_his_id_)
                historyUrl = x["historyUrl"]
                #print(historyUrl)
                previewUrl = x["previewUrl"]
                #print(previewUrl)
                privatePreviewUrl = x["privatePreviewUrl"]
                counter += 1


        resp = requests.get('https://{}.salesforce.com{}'.format(server_id,previewUrl), headers=headers)

        #formatted_response = html.unescape(resp.text)
        formatted_response = json.loads(resp.text)
        formatted_response_str = json.dumps(formatted_response, indent=2)
        dataflow_ = formatted_response.get('definition')
        #print(formatted_response)

        for x in dataflow_:
            value = dataflow_[x]
            #print('{} {}'.format(x,value))

        with open('{}_dataflow_backup.json'.format(dataflow_name_), 'w') as outfile:
            json.dump(dataflow_, outfile)

        #quit()
        os_running = get_platform()

        if os_running == "Windows":

            #bat_ = open("replace.bat", "w")
            #a_ = "PowerShell -windowstyle hidden -NoProfile -ExecutionPolicy bypass -Command \"& {Start-Process PowerShell -windowstyle hidden -ArgumentList '-windowstyle hidden -NoProfile -ExecutionPolicy bypass -File \"\"replace.ps1\"\"'}\""
            #bat_.write(a_)
            #bat_.close()
            #time.sleep(0.1)

            a_ = "(Get-Content " + '{}_dataflow_backup.json'.format(dataflow_name_) + ").Replace('&quot;','\\\"') | Set-Content " + '{}_dataflow_backup.json'.format(dataflow_name_)
            b_ = "(Get-Content " + '{}_dataflow_backup.json'.format(dataflow_name_) + ").Replace('&#39;','''') | Set-Content " + '{}_dataflow_backup.json'.format(dataflow_name_)
            c_ = "(Get-Content " + '{}_dataflow_backup.json'.format(dataflow_name_) + ").Replace('&gt;','>') | Set-Content " + '{}_dataflow_backup.json'.format(dataflow_name_)
            d_ = "(Get-Content " + '{}_dataflow_backup.json'.format(dataflow_name_) + ").Replace('&lt;','<') | Set-Content " + '{}_dataflow_backup.json'.format(dataflow_name_)
            e_ = "(Get-Content " + '{}_dataflow_backup.json'.format(dataflow_name_) + ").Replace('&amp;','&') | Set-Content " + '{}_dataflow_backup.json'.format(dataflow_name_)
            ps_1_dict = {"a": "{}".format(a_), "b": "{}".format(b_), "c": "{}".format(c_), "d": "{}".format(d_), "e": "{}".format(e_)}

            for x in ps_1_dict.values():
                #print(x)
                completed = subprocess.run(["powershell", "-Command", x], capture_output=True)
                #ps1_ = open("replace.ps1", "w")
                #time.sleep(0.1)
                #ps1_.write(x)
                #time.sleep(0.1)
                #ps1_.close()
                #time.sleep(0.1)
                #replace_ = subprocess.run(["replace.bat"], stdout=subprocess.PIPE, text=True, shell=True)
                #print(replace_.stdout)
                time.sleep(0.5)

            #os.remove("replace.bat")
            #os.remove("replace.ps1")


        prCyan("\r\n" + "Dataflow JSON definition succesfully backed up here: ")
        prLightPurple("\r\n" + "{}".format(d_ext) + "\r\n")

        time.sleep(2)

        os.chdir("..")
    else:
        prRed("\r\n" + "There is no JSON available to backup." + "\r\n")
        os.chdir("..")
        time.sleep(2)
