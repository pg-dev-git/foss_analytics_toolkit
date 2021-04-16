import json
import requests
from terminal_colors import *
from sfdc_login import *
import os
import time


def dataset_history(access_token,dataset_,server_id,versionsUrl):
    headers = {
        'Authorization': "Bearer {}".format(access_token),
        'Content-Type': "application/json"
        }
    resp = requests.get('https://{}.salesforce.com{}'.format(server_id,versionsUrl), headers=headers)

    formatted_response = json.loads(resp.text)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    prGreen(formatted_response_str)

    dataset_his_list = formatted_response.get('versions')

    #Check if there are available histories to backup - start:

    check_counter = 0

    for x in dataset_his_list:
        check_counter += 1

    #Check if there are available histories to backup - end.

    if check_counter != 0:

        counter_2 = 0

        counter = 0

        for x in dataset_his_list:
            counter += 1
            print("{}- ".format(counter) ,"Version id:",x["id"],"Total Rows:",x["totalRows"],"- Created On:",x["createdDate"],"- Last Modified On:",x["lastModifiedDate"])

        time.sleep(2)
        prYellow("\r\n" + "#1 is your active version of the Dataset." + "\r\n")

        ####action_track = input("Choose a Dataflow History id between #2 and {} to replace the current version or hit any other key to go back:".format(counter))

        counter_2 = 0

    else:
        prRed("\r\n" + "There are no history records available." + "\r\n")
        time.sleep(2)
