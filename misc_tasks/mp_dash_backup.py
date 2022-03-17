import json, requests, time, sys, subprocess, datetime, shutil, os, html, re, multiprocessing as mp
from misc_tasks.terminal_colors import *
from misc_tasks.sfdc_login import *
from dashboards_tasks.get_dash_datasets import *
from misc_tasks.zipper import *
from misc_tasks.line import *

def remove(string):
    return string.replace(" ", "_")

def mp_dash_backup(access_token,server_id,i,dashboards_list,server_domain):

    try:
        x = i
        dashboard_ = dashboards_list[x]["id"]
        dashboard_label = dashboards_list[x]["label"]
        historiesUrl = dashboards_list[x]["historiesUrl"]

        string = dashboard_label

        try:
            dash_name = remove(string)
        except:
            dash_name = dashboard_label

        try:
            os.remove('{}_{}_backup.json'.format(dash_name,dashboard_))
        except:
            pass

        headers = {
            'Authorization': "Bearer {}".format(access_token)
            }
        resp = requests.get('https://{}.my.salesforce.com/services/data/v54.0/wave/dashboards/{}'.format(server_domain,dashboard_), headers=headers)

        formatted_response = json.loads(resp.text)

        formatted_response_str = json.dumps(formatted_response, indent=2)

        state_ = formatted_response.get("state")
        label_ = formatted_response.get("label")
        mobileDisabled_ = formatted_response.get("mobileDisabled")
        datasets_ = formatted_response.get("datasets")

        json_backup = {}

        json_backup['label'] = label_
        json_backup['mobileDisabled'] = mobileDisabled_
        json_backup['state'] = state_
        json_backup['datasets'] = datasets_

        with open('{}_{}_backup.json'.format(dash_name,dashboard_), 'w') as outfile:
            json.dump(json_backup, outfile)

        try:
            json_bkp_unscapped = open('{}_{}_backup.json'.format(dash_name,dashboard_), 'r')
            json_bkp_unscapped = (json_bkp_unscapped.read())

            regexp = "&.+?;"
            _html = re.findall(regexp, json_bkp_unscapped)
            for e in _html:
                unescaped = html.unescape(e)
                json_bkp_unscapped = json_bkp_unscapped.replace(e, unescaped)

            json_bkp_unscapped = json_bkp_unscapped.replace('"query": {"query":','"query":')
            json_bkp_unscapped = json_bkp_unscapped.replace(', "version": -1.0}','')
            json_bkp_unscapped = json_bkp_unscapped.replace('"{"','{"')
            json_bkp_unscapped = json_bkp_unscapped.replace('}",','},')
            json_bkp_unscapped = json_bkp_unscapped.replace('"}"','"}')

            with open('{}_{}_backup.json'.format(dash_name,dashboard_), 'w') as outfile:
                json.dump(json.loads(json_bkp_unscapped), outfile)
        except:
            pass

        prLightPurple("{} ".format(dashboard_label))
    except:
        traceback.print_exc()
