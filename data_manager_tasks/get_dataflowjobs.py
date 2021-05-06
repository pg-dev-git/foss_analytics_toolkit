import json
import requests
from terminal_colors import *
import time
from data_manager_tasks.get_dataflowjobs_list import *
from line import *

def get_dataflowsJobs(access_token,server_id):
    prGreen("\r\n" + "Checking Data Manager Logs..." + "\r\n")
    time.sleep(1)
    headers = {
        'Authorization': "Bearer {}".format(access_token,server_id)
        }
    resp = requests.get('https://{}.salesforce.com/services/data/v51.0/wave/dataflowjobs'.format(server_id), headers=headers)
    #print(resp.json())
    #Print PrettyJSON in Terminal

    formatted_response = json.loads(resp.text)
    #print(formatted_response)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    #prGreen(formatted_response_str)

    dataflowjobs_list = formatted_response.get('dataflowJobs')

    run_token = True

    try:
        while run_token:
            line_print()
            prYellow("Choose an option from the list below:" + "\r\n")
            prCyan("1 - List Dataflow Jobs by Status")
            prCyan("2 - List Datasync Jobs by Status")
            prCyan("3 - List All Jobs by Status")

            #prCyan("5 - Upload a CSV Dataset - New/Override")
            print("\r\n")
            user_input = input("Enter your selection: ")
            line_print()

            if user_input == "1":
                get_dataflowsJobs_list(dataflowjobs_list)

            if user_input == "2":
                get_datasyncJobs_list(dataflowjobs_list)

            if user_input == "3":
                get_AllJobs_list(dataflowjobs_list)

            if user_input == "4":
                get_dataflowsJobs(access_token)

            #if user_input == "5":
            #    get_datasyncJobs(access_token)

            print("\r\n")

            check_token = input("Do you want to do something else (Y) or go back (N)?" + "\r\n")

            if check_token == "Y" or check_token == "y":
                run_token = True
            elif check_token == "N" or check_token == "n":
                run_token = False
    except ValueError:
        prRed("Wrong selection. Going back.")
