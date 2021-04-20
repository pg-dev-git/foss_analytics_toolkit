import json
import requests
from terminal_colors import *
from sfdc_login import *
from dashboards_tasks.get_dash_datasets import *
import time

def backup_dash_json(access_token,dashboard_,server_id,dashboard_label):
    prGreen("\r\n" + "Getting dashboards list..." + "\r\n")
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

    with open('{}_{}_backup.json'.format(dashboard_label,dashboard_), 'w') as outfile:
        json.dump(json_backup, outfile)
