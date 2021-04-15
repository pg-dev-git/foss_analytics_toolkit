import json
import requests
from terminal_colors import *
from sfdc_login import *
import os
from dataflow_tasks.start_stop_dataflow import *
from dataflow_tasks.get_dataflow_history import *
from dataflow_tasks.backup_current import *


def get_dataflows(access_token,server_id):
    prGreen("\r\n" + "getting dataflows list..." + "\r\n")
    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }
    resp = requests.get('https://{}.salesforce.com/services/data/v51.0/wave/dataflows'.format(server_id), headers=headers)
    #print(resp.json())

    #Print PrettyJSON in Terminal
    formatted_response = json.loads(resp.text)
    #print(formatted_response)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    #prGreen(formatted_response_str)

    dataflow_list = formatted_response.get('dataflows')

    counter = 0

    dataflow_id_ = 999999999

    for x in dataflow_list:
        counter += 1
        print("{} - ".format(counter) ,"Dataflow id: ",x["id"]," - Label: ",x["label"])
    print("\r\n")

    action_track = input("Choose a Dataflow # (1 - {}) to view more actions or hit any other key to go back:".format(counter))

    counter_2 = 0

    try:
        action_track = int(action_track)
        if type(action_track) == int and action_track > 0 and action_track <= counter:

            for x in dataflow_list:
                counter_2 += 1
                if counter_2 == action_track:
                    dataflow_id_ = x["id"]
                    dataflow_name_ = x["name"]
                    dataflow_his_url = x["historiesUrl"]

            prYellow("\r\n" + "Dataset selected: {} - {}".format(dataflow_name_, dataflow_id_) + "\r\n")

            run_token = True
            while run_token:
                prGreen("What do you want to do?:")
                prYellow("(Choose a number from the list below)" + "\r\n")
                prCyan("1 - Start Dataflow")
                prCyan("2 - Stop Dataflow (if previously triggered from option 1)")
                prCyan("3 - Show Dataflow Version History (#1 on the list is the current version.)")
                prCyan("4 - Backup Current version")

                user_input = input("\r\n" + "Enter your selection: ")

                print("\r\n")

                if user_input == "1":
                    start_dataflow(access_token,dataflow_id_,server_id)

                if user_input == "2":
                    stop_dataflow(access_token,dataflow_id_,server_id)

                if user_input == "3":
                    get_dataflow_history(access_token,dataflow_his_url,dataflow_id_,server_id)

                if user_input == "4":
                    backup_dataflow_current(access_token,dataflow_his_url,dataflow_id_,dataflow_name_,server_id)

                #if user_input == "5":
                #    get_dataset_details(access_token,server_id)

                check_token = input("Press \"Y\" to see the dataset actions or hit any key to go back" + "\r\n")

                if check_token == "Y":
                    run_token = True
                else:
                    run_token = False

    except ValueError:
        prRed("Please use an integer.")
