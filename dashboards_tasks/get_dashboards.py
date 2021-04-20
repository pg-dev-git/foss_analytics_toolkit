import json
import requests
from terminal_colors import *
from sfdc_login import *
from dashboards_tasks.get_dash_datasets import *
from dashboards_tasks.backup_dash_json import *
from dashboards_tasks.get_dashboard_history import *
import time

def get_dashboards(access_token,server_id):
    prGreen("\r\n" + "Getting dashboards list..." + "\r\n")
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

    prCyan("\r\n" + "Dashboards:")
    time.sleep(1)

    for x in dashboards_list:
        counter += 1
        if counter >= 1 and counter <= 9:
            print(" {} - ".format(counter) ,"Dashboard id:",x["id"],"- Label:",x["label"])
        else:
            print("{} - ".format(counter) ,"Dashboard id:",x["id"],"- Label:",x["label"])
    print("\r\n")

    dataset_ = 999999999
    action_track = input("Enter a dashboard # (1 - {}) to view more actions or hit any other key to go back:".format(counter))

    counter_2 = 0

    counter_3 = 0

    try:
        action_track = int(action_track)
        if type(action_track) == int and action_track > 0 and action_track <= counter:

            for x in dashboards_list:
                counter_2 += 1
                if counter_2 == action_track:
                    dashboard_ = x["id"]
                    dashboard_label = x["label"]
                    historiesUrl = x["historiesUrl"]

            run_token = True
            while run_token:
                prGreen("What do you want to do?:")
                time.sleep(0.3)
                prYellow("(Choose a number from the list below)" + "\r\n")
                time.sleep(0.5)
                prCyan("1 - List Dashboard Datasets")
                time.sleep(0.15)
                prCyan("2 - Backup Dashboard JSON")
                time.sleep(0.15)
                prCyan("3 - Override dataset")
                time.sleep(0.15)
                prCyan("4 - Append to dataset")
                time.sleep(0.15)
                prCyan("5 - Backup User XMD")
                time.sleep(0.15)
                prCyan("6 - Show Version History")
                time.sleep(0.15)
                prCyan("7 - Show Dependencies")
                time.sleep(0.15)
                prRed("8 - Delete dataset")
                time.sleep(0.5)

                user_input = input("\r\n" + "Enter your selection: ")

                if user_input == "1":
                    get_dash_datasets(access_token,dashboard_,server_id)

                if user_input == "2":
                    backup_dash_json(access_token,dashboard_,server_id,dashboard_label)

                if user_input == "3":
                    upload_csv_dataset(access_token,dataset_name,dataset_,server_id)

                if user_input == "4":
                    append_csv_dataset(access_token,dataset_name,dataset_,server_id)

                if user_input == "5":
                    backup_xmd_user(access_token,dataset_,server_id)

                if user_input == "6":
                    dashboard_history(access_token,dashboard_,server_id,historiesUrl)

                if user_input == "7":
                    dataset_dependencies(access_token,dataset_,server_id)

                if user_input == "8":
                    delete_dataset(access_token,dataset_,server_id)

                prCyan("\r\n" + "Selected Dashboard: {} - {}".format(dashboard_, dashboard_label))
                check_token = input("Press \"Y\" to see the dashboard actions or hit any key to go back" + "\r\n")

                if check_token == "Y":
                    run_token = True
                else:
                    run_token = False
                    prYellow("\r\n" + "Going back to the previous menu.")
                    time.sleep(2)

    except ValueError:
        prYellow("\r\n" + "Going back to the previous menu.")
        time.sleep(2)
