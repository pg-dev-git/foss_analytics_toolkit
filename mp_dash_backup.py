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


def mp_dash_backup(access_token,server_id,i,dashboards_list,headers):

    x = i
    #print(x)
    dashboard_ = dashboards_list[x]["id"]
    dashboard_label = dashboards_list[x]["label"]
    historiesUrl = dashboards_list[x]["historiesUrl"]
    #print(dashboard_,dashboard_label,historiesUrl)

    string = dashboard_label

    try:
        dash_name = remove(string)
    except:
        dash_name = dashboard_label

    #print(dash_name)

    try:
        os.remove('{}_{}_backup.json'.format(dash_name,dashboard_))
    except:
        pass

    #print("make request")

    resp = requests.get('https://{}.salesforce.com/services/data/v50.0/wave/dashboards/{}'.format(server_id,dashboard_), headers=headers)
    #print(resp.text)

    formatted_response = json.loads(resp.text)
    #print(formatted_response)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    #prGreen(formatted_response_str)

    state_ = formatted_response.get("state")
    label_ = formatted_response.get("label")
    mobileDisabled_ = formatted_response.get("mobileDisabled")
    datasets_ = formatted_response.get("datasets")

    #print(state_)

    #prGreen(datasets_)

    json_backup = {}

    json_backup['label'] = label_
    json_backup['mobileDisabled'] = mobileDisabled_
    json_backup['state'] = state_
    json_backup['datasets'] = datasets_

    with open('{}_{}_backup.json'.format(dash_name,dashboard_), 'w') as outfile:
        json.dump(json_backup, outfile)

    os_running = get_platform()

    if os_running == "Windows":

        a_ = "(Get-Content " + '{}_{}_backup.json'.format(dash_name,dashboard_) + ").Replace('&quot;','\\\"') | Set-Content " + '{}_{}_backup.json'.format(dash_name,dashboard_)
        b_ = "(Get-Content " + '{}_{}_backup.json'.format(dash_name,dashboard_) + ").Replace('&#39;','''') | Set-Content " + '{}_{}_backup.json'.format(dash_name,dashboard_)
        c_ = "(Get-Content " + '{}_{}_backup.json'.format(dash_name,dashboard_) + ").Replace('&gt;','>') | Set-Content " + '{}_{}_backup.json'.format(dash_name,dashboard_)
        d_ = "(Get-Content " + '{}_{}_backup.json'.format(dash_name,dashboard_) + ").Replace('&lt;','<') | Set-Content " + '{}_{}_backup.json'.format(dash_name,dashboard_)
        e_ = "(Get-Content " + '{}_{}_backup.json'.format(dash_name,dashboard_) + ").Replace('&amp;','&') | Set-Content " + '{}_{}_backup.json'.format(dash_name,dashboard_)
        ps_1_dict = {"a": "{}".format(a_), "b": "{}".format(b_), "c": "{}".format(c_), "d": "{}".format(d_), "e": "{}".format(e_)}

        for x in ps_1_dict.values():
            completed = subprocess.run(["powershell", "-Command", x], capture_output=True)
            time.sleep(0.15)

    print("\r\n" + "Done backing up:")
    prLightPurple("{} ".format(dashboard_label))
