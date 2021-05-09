import json
import requests
from terminal_colors import *
from sfdc_login import *
from dashboards_tasks.get_dash_datasets import *
import time
import sys
import subprocess
from zipper import *

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

def backup_dash_json(access_token,dashboard_,server_id,dashboard_label):

    try:
        dataflow_extraction_dir = "dashboard_backup"
        os.mkdir(dataflow_extraction_dir)
    except OSError as error:
            pass

    cd = os.getcwd()

    d_ext = "{}".format(cd)+"\\dashboard_backup\\"

    os.chdir(d_ext)

    prGreen("\r\n" + "Getting Dashboard JSON Definition..." + "\r\n")
    time.sleep(1)
    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }
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

    string = dashboard_label

    try:
        dash_name = remove(string)
    except:
        dash_name = dashboard_label

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
            #print(x)
            completed = subprocess.run(["powershell", "-Command", x], capture_output=True)
            time.sleep(0.5)

        #directory = './'
        #tcrm_zipper(directory)
        prCyan("\r\n" + "Dashboard JSON definition succesfully backed up here: ")
        prLightPurple("\r\n" + "{}".format(d_ext) + "\r\n")
        line_print()
        time.sleep(2)

        prYellow("\r\n" + "Dashboard selected: {} - {}".format(dashboard_label,dashboard_) + "\r\n")

        os.chdir("..")
    else:
        prRed("\r\n" + "There is no JSON available to backup." + "\r\n")
        os.chdir("..")
        line_print()
        time.sleep(2)
        prYellow("\r\n" + "Dashboard selected: {} - {}".format(dashboard_label,dashboard_) + "\r\n")
