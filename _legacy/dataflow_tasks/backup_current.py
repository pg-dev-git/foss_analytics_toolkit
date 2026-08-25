import json, os, requests, time, html, sys, subprocess, traceback, html, re
from misc_tasks.terminal_colors import *
from misc_tasks.sfdc_login import *
from misc_tasks.line import *

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

def backup_dataflow_current(access_token,dataflow_his_url,dataflow_id_,dataflow_name_,server_id,server_domain):

    try:
        dataflow_extraction_dir = "dataflow_backup"
        os.mkdir(dataflow_extraction_dir)
    except OSError as error:
            pass

    cd = os.getcwd()

    os_ = sfdc_login.get_platform()

    if os_ == "Windows":
        d_ext = "{}".format(cd)+"\\dataflow_backup\\"
    else:
        d_ext = "{}".format(cd)+"/dataflow_backup/"

    os.chdir(d_ext)

    previewUrl = "kusdyu232dusgx"

    try:
        os.remove('{}_dataflow_backup.json'.format(dataflow_name_))
    except:
        pass

    headers = {
        'Authorization': "Bearer {}".format(access_token),
        'Content-Type': "application/json"
        }
    resp = requests.get('https://{}.my.salesforce.com{}'.format(server_domain,dataflow_his_url), headers=headers)

    formatted_response = json.loads(resp.text)
    formatted_response_str = json.dumps(formatted_response, indent=2)
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
                counter += 1


        resp = requests.get('https://{}.my.salesforce.com{}'.format(server_domain,previewUrl), headers=headers)

        formatted_response = json.loads(resp.text)
        formatted_response_str = json.dumps(formatted_response, indent=2)
        dataflow_ = formatted_response.get('definition')

        for x in dataflow_:
            value = dataflow_[x]

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


        prCyan("\r\n" + "Dataflow JSON definition succesfully backed up here: ")
        prLightPurple("\r\n" + "{}".format(d_ext) + "\r\n")
        line_print()

        time.sleep(2)

        prYellow("\r\n" + "Dataflow selected: {} - {}".format(dataflow_name_, dataflow_id_) + "\r\n")

        os.chdir("..")
    else:
        prRed("\r\n" + "There is no JSON available to backup." + "\r\n")
        os.chdir("..")
        line_print()
        time.sleep(2)
        prYellow("\r\n" + "Dataflow selected: {} - {}".format(dataflow_name_, dataflow_id_) + "\r\n")
