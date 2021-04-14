import json
import requests
from terminal_colors import *
from sfdc_login import *
from dataset_tasks.get_dataset_field_detail import *
from dataset_tasks.get_dataset_extract import *
from dataset_tasks.upload_dataset import *
from dataset_tasks.dataset_backup_user_xmd import *
from dataset_tasks.append_dataset import *
from dataset_tasks.delete_dataset import *

def get_datasets(access_token,server_id):
    prGreen("\r\n" + "getting datasets list..." + "\r\n")
    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }
    resp = requests.get('https://{}.salesforce.com/services/data/v51.0/wave/datasets'.format(server_id), headers=headers)
    #print(resp.json())
    #Print PrettyJSON in Terminal

    formatted_response = json.loads(resp.text)
    #print(formatted_response)
    #formatted_response_str = json.dumps(formatted_response, indent=2)
    #prGreen(formatted_response_str)

    datasets_list = formatted_response.get('datasets')

    counter = 0

    for x in datasets_list:
        counter += 1
        print("{} - ".format(counter) ,"Datset id: ",x["id"]," - Label: ",x["label"])
    print("\r\n")

    dataset_ = 999999999
    action_track = input("Enter a dataset # (1 - {}) to view more actions or hit any other key to go back:".format(counter))

    counter_2 = 0

    counter_3 = 0

    try:
        action_track = int(action_track)
        if type(action_track) == int and action_track > 0 and action_track <= counter:

            for x in datasets_list:
                counter_2 += 1
                if counter_2 == action_track:
                    dataset_ = x["id"]

            for x in datasets_list:
                counter_3 += 1
                if counter_3 == action_track:
                    dataset_name = x["name"]

            run_token = True
            while run_token:
                prGreen("What do you want to do?:")
                prYellow("(Choose a number from the list below)" + "\r\n")
                prCyan("1 - List dataset fields")
                prCyan("2 - Extract dataset")
                prCyan("3 - Override dataset")
                prCyan("4 - Append to dataset")
                prCyan("5 - Backup User XMD")
                prRed("6 - Delete dataset")

                user_input = input("Enter your selection: ")

                if user_input == "1":
                    get_datasets_field_details(access_token,dataset_,server_id)

                if user_input == "2":
                    get_datasets_extract(access_token,dataset_,server_id)

                if user_input == "3":
                    upload_csv_dataset(access_token,dataset_name,dataset_,server_id)

                if user_input == "4":
                    append_csv_dataset(access_token,dataset_name,dataset_,server_id)

                if user_input == "5":
                    backup_xmd_user(access_token,dataset_,server_id)

                if user_input == "6":
                    delete_dataset(access_token,dataset_,server_id)

                check_token = input("Press \"Y\" to see the dataset actions or hit any key to go back" + "\r\n")

                if check_token == "Y":
                    run_token = True
                else:
                    run_token = False

    except ValueError:
        prRed("Please use an integer.")
