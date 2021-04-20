import json
import requests
from terminal_colors import *
from sfdc_login import *
import os
import time


def dashboard_history(access_token,dashboard_,server_id,historiesUrl):
    headers = {
        'Authorization': "Bearer {}".format(access_token),
        'Content-Type': "application/json"
        }
    resp = requests.get('https://{}.salesforce.com{}'.format(server_id,historiesUrl), headers=headers)

    formatted_response = json.loads(resp.text)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    prGreen(formatted_response_str)

    dash_his_list = formatted_response.get('histories')

    #Check if there are available histories to backup - start:

    check_counter = 0

    for x in dash_his_list:
        check_counter += 1

    #Check if there are available histories to backup - end.

    if check_counter != 0:

        counter_2 = 0

        counter = 0

        for x in dash_his_list:
            counter += 1
            print("{}- ".format(counter) ,"History id:",x["id"],"- Created On:",x["createdDate"],"- Label:",x["label"])

        time.sleep(2)
        prYellow("\r\n" + "#1 is the latest version of the Dashboard." + "\r\n")

        action_track = "999"
        try:
            action_track = input("Choose a Dashboard History id between #2 and {} to replace the current version or hit any other key to go back:".format(counter))
            action_track = int(action_track)
            while int(action_track) == 999 or int(action_track) == 1 or int(action_track) > counter:
                prYellow("\r\n" + "Please enter a number between 2 and {}".format(counter) + "\r\n")
                time.sleep(2)
                action_track = input("Choose a Dashboard History id between #2 and {} to replace the current version or hit any other key to go back:".format(counter))
            try:
                action_track = int(action_track)
                if type(action_track) == int and action_track > 0 and action_track <= counter:

                    for x in dash_his_list:
                        counter_2 += 1
                        if counter_2 == action_track:
                            dash_his_id_ = x["id"]
                            dash_his_label_ = x["label"]
                            dash_his_date_ = x["createdDate"]
                            revertUrl_ = x["revertUrl"]

                    time.sleep(1)

                    prYellow("\r\n" + "Version selected: Id:{} - Created On:{} - Label:{}".format(dash_his_id_,dash_his_date_,dash_his_label_) + "\r\n")

                    time.sleep(1)

                    prRed("Do you want to replace your current Dashboard with the version selected?")

                    time.sleep(1)

                    action_track = input("Press \"Y\" to confirm or hit any other key to cancel: ")

                    time.sleep(1)

                    historylabel = input("Enter a short History Label for this update: ")


                    try:
                        if action_track == "Y":
                            headers = {
                                'Authorization': "Bearer {}".format(access_token),
                                'Content-Type': "application/json"
                                }

                            payload = {"historyId": "{}".format(dash_his_id_), "historyLabel": "{}".format(historylabel)}

                            payload = json.dumps(payload)

                            resp = requests.put('https://{}.salesforce.com/services/data/v51.0/wave/dashboards/{}/bundle'.format(server_id,dashboard_), headers=headers, data=payload)
                            #formatted_response = json.loads(resp.text)
                            #formatted_response_str = json.dumps(formatted_response, indent=2)
                            #prGreen(formatted_response_str)

                            prYellow("\r\n" + "Dashboard version replaced." + "\r\n")
                            time.sleep(2)
                        else:
                            prRed("\r\n" + "Dataflow replacement cancelled." + "\r\n")
                            time.sleep(2)

                    except ValueError:
                        prRed("\r\n" + "Dataflow replacement cancelled." + "\r\n")
                        time.sleep(2)


            except ValueError:
                prYellow("\r\n" + "Please enter a number between 2 and {}".format(counter) + "\r\n")
                time.sleep(2)
        except ValueError:
            prYellow("\r\n" + "Wrong input. Cancelling operation." + "\r\n")
            time.sleep(2)

        counter_2 = 0
    else:
        prRed("\r\n" + "There are no history records available." + "\r\n")
        time.sleep(2)
