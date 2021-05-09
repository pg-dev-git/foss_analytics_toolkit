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

def mass_dashboards(access_token,server_id):

    now = datetime.datetime.now()

    dt_string = now.strftime("%d-%m-%Y_%H_%M")

    try:
        dataflow_extraction_dir = "dashboard_backup"
        os.mkdir(dataflow_extraction_dir)
    except OSError as error:
            pass

    cd = os.getcwd()

    d_ext = "{}".format(cd)+"\\dashboard_backup\\"

    os.chdir(d_ext)

    try:
        dataflow_extraction_dir = 'mass_dashboard_backup_{}'.format(dt_string)
        os.mkdir(dataflow_extraction_dir)
    except OSError as error:
            pass

    cd = os.getcwd()

    d_ext = "{}".format(cd)+"\\{}\\".format(dataflow_extraction_dir)

    os.chdir(d_ext)

    prGreen("\r\n" + "Getting dashboards list..." + "\r\n")
    line_print()

    time.sleep(1)

    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }
    resp = requests.get('https://{}.salesforce.com/services/data/v51.0/wave/dashboards'.format(server_id), headers=headers)
    #print(resp.json())
    #Print PrettyJSON in Terminal

    formatted_response = json.loads(resp.text)
    #print(formatted_response)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    #prGreen(formatted_response_str)

    dashboards_list = formatted_response.get('dashboards')

    counter = 0

    for x in dashboards_list:
        counter += 1

    if counter == 1:
        prGreen("\r\n" + "{} Dashboard will be backed up now...".format(counter) + "\r\n")
    else:
        prGreen("\r\n" + "{} Dashboards will be backed up now...".format(counter) + "\r\n")

    line_print()

    _start = time.time()

    i = 0

    if counter !=0:
        for x in dashboards_list:
            i += 1
            print("{} - ".format(i) ,"Dashboard id: ",x["id"]," - Label: ",x["label"])
            dashboard_ = x["id"]
            dashboard_label = x["label"]
            historiesUrl = x["historiesUrl"]

            try:
                os.remove('{}_backup.json'.format(dataflow_name_))
            except:
                pass

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
                    time.sleep(0.25)

            prGreen("\r\n" + "Backup Successfull. ")
            line_print()
    else:
        prRed("\r\n" + "There aren't Dashboards available to backup." + "\r\n")
        line_print()
        time.sleep(0.15)

    _end = time.time()

    total_time = round((_end - _start),2)

    os.chdir("..")
    directory = './{}'.format(dataflow_extraction_dir)
    tcrm_zipper(directory,dataflow_extraction_dir)
    shutil.rmtree(r'./{}'.format(dataflow_extraction_dir))
    prGreen("\r\n" + "Mass Backup succesfully completed in {}s.".format(total_time))
    time.sleep(0.15)
    cd = os.getcwd()
    prCyan("\r\n" + "Find the files here: ")
    prLightPurple("\r\n" + "{}".format(cd))
    #line_print()


    os.chdir("..")
