import json
import requests
from terminal_colors import *
from sfdc_login import *
import os
import time


def get_dataflow_history(access_token,dataflow_his_url,dataflow_id_,server_id):
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

    check_counter = 0

    for x in dataflow_his_list:
        check_counter += 1

    #Check if there are available histories to backup - end.

    if check_counter != 0:

        counter_2 = 0

        counter = 0

        for x in dataflow_his_list:
            counter += 1
            print("{}- ".format(counter) ,"History id:",x["id"],"- Name: ",x["name"],"- Label:",x["label"],"- Created On:",x["createdDate"])

        time.sleep(2)
        prYellow("\r\n" + "#1 is your active version of the Dataflow." + "\r\n")

        action_track = input("Choose a Dataflow History id between #2 and {} to replace the current version or hit any other key to go back:".format(counter))

        counter_2 = 0

        try:
            action_track = int(action_track)
            if type(action_track) == int and action_track > 0 and action_track <= counter:

                for x in dataflow_his_list:
                    counter_2 += 1
                    if counter_2 == action_track:
                        dataflow_his_id_ = x["id"]
                        dataflow_his_name_ = x["name"]
                        dataflow_his_label_ = x["label"]
                        dataflow_his_date_ = x["createdDate"]

                time.sleep(1)

                prYellow("\r\n" + "Version selected: Id:{} - Name:{} - Label:{} - Created On:{}".format(dataflow_his_id_, dataflow_his_name_,dataflow_his_label_,dataflow_his_date_) + "\r\n")

                time.sleep(1)

                prRed("Do you want to replace your current Dataflow with the version selected?")

                time.sleep(1)

                action_track = input("Press \"Y\" to confirm or hit any other key to cancel:")

                try:
                    if action_track == "Y" or action_track == "y":
                        headers = {
                            'Authorization': "Bearer {}".format(access_token),
                            'Content-Type': "application/json"
                            }

                        payload = {"historyId": "{}".format(dataflow_his_id_)}

                        payload = json.dumps(payload)

                        resp = requests.put('https://{}.salesforce.com/services/data/v51.0/wave/dataflows/{}'.format(server_id,dataflow_id_), headers=headers, data=payload)

                        prYellow("\r\n" + "Dataflow version replaced." + "\r\n")
                        time.sleep(2)
                except ValueError:
                    prRed("\r\n" + "Dataflow replacement cancelled." + "\r\n")
                    time.sleep(2)


        except ValueError:
            prYellow("\r\n" + "Going back to the previous menu." + "\r\n")
            time.sleep(2)
    else:
        prRed("\r\n" + "There are no history records available." + "\r\n")
        time.sleep(2)
