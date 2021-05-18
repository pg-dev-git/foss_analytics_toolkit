import json
import requests
from terminal_colors import *
from sfdc_login import *
from dataset_tasks.get_dataset_field_detail import *
from dataset_tasks.get_dataset_extract_v2 import *
from dataset_tasks.get_dataset_extract_MP import *
from dataset_tasks.upload_dataset import *
from dataset_tasks.dataset_backup_user_xmd import *
from dataset_tasks.xmd_cleanup import *
from dataset_tasks.append_dataset import *
from dataset_tasks.delete_dataset import *
from dataset_tasks.get_dataset_history import *
from dataset_tasks.get_dataset_dependencies import *
import time
from line import *
import queue
import threading


def get_datasets(access_token,server_id):
    prGreen("\r\n" + "Getting datasets list..." + "\r\n")
    line_print()
    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }
    resp = requests.get('https://{}.salesforce.com/services/data/v51.0/wave/datasets'.format(server_id), headers=headers)
    #print(resp.json())
    #Print PrettyJSON in Terminal

    formatted_response = json.loads(resp.text)
    #print(formatted_response)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    #prGreen(formatted_response_str)

    datasets_list = formatted_response.get('datasets')

    counter = 0
    counterx = 0

    for xx in datasets_list:
        counterx += 1

    i = 0

    que = queue.Queue()
    threads = list()

    t_result = []

    for index in range(counterx):
        cvl = datasets_list[index]["currentVersionUrl"]
        params = [server_id,access_token,cvl,i]
        x = threading.Thread(target=lambda q, arg1: q.put(dataset_list_mt(arg1)), args=(que,params))
        threads.append(x)
        x.start()
        time.sleep(0.15)


    for index, thread in enumerate(threads):
        thread.join()
        time.sleep(0.1)

    while not que.empty():
        t_result.append(que.get())

    prCyan("\r\n" + "Datasets:" + "\r\n")

    for x in range(counterx):
        counter += 1
        if counter >= 1 and counter <= 9:
            print("  {} - ".format(counter) ,"Dataset id: ",datasets_list[x]["id"]," - Rows: ",t_result[x]," - Label: ",datasets_list[x]["label"])
        elif counter > 9 and counter <= 99:
            print(" {} - ".format(counter) ,"Dataset id: ",datasets_list[x]["id"]," - Rows: ",t_result[x]," - Label: ",datasets_list[x]["label"])
        else:
            print("{} - ".format(counter) ,"Dataset id: ",datasets_list[x]["id"]," - Rows: ",t_result[x]," - Label: ",datasets_list[x]["label"])

    print("\r\n")

    dataset_ = 999999999
    action_track = input("Enter a dataset # (1 - {}) to view more actions or hit any other key to go back: ".format(counter))

    counter_2 = 0

    counter_3 = 0

    try:
        action_track = int(action_track)
        if type(action_track) == int and action_track > 0 and action_track <= counter:

            for x in datasets_list:
                counter_2 += 1
                if counter_2 == action_track:
                    dataset_ = x["id"]
                    dataset_name = x["name"]
                    versionsUrl = x["versionsUrl"]

            run_token = True
            while run_token:
                line_print()
                prGreen("\r\n" + "What do you want to do?:")
                time.sleep(0.3)
                prYellow("(Choose a number from the list below)" + "\r\n")
                time.sleep(0.5)
                prCyan("1 - List dataset fields")
                time.sleep(0.15)
                prCyan("2 - Extract dataset")
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
                prCyan("8 - XMD Cleanup")
                time.sleep(0.15)
                prRed("9 - Delete dataset")
                time.sleep(0.5)

                user_input = input("\r\n" + "Enter your selection: ")
                line_print()

                if user_input == "1":
                    get_datasets_field_details(access_token,dataset_,server_id,dataset_name)

                if user_input == "2":
                    #get_datasets_extract(access_token,dataset_,server_id,dataset_name)
                    get_datasets_extract_mp(access_token,dataset_,server_id)

                if user_input == "3":
                    upload_csv_dataset(access_token,dataset_name,dataset_,server_id,dataset_name)

                if user_input == "4":
                    append_csv_dataset(access_token,dataset_name,dataset_,server_id,dataset_name)

                if user_input == "5":
                    backup_xmd_user(access_token,dataset_,server_id,dataset_name)

                if user_input == "6":
                    dataset_history(access_token,dataset_,server_id,versionsUrl,dataset_name)

                if user_input == "7":
                    dataset_dependencies(access_token,dataset_,server_id,dataset_name)

                if user_input == "8":
                    xmd_cleanup(access_token,dataset_,server_id,dataset_name)

                if user_input == "9":
                    delete_dataset(access_token,dataset_,server_id)

                check_token = input("\r\n" + "Press \"Y\" to see the dataset actions or hit any key to go back" + "\r\n")

                if check_token == "Y" or check_token == "y":
                    run_token = True
                else:
                    run_token = False
                    prYellow("\r\n" + "Going back to the previous menu.")
                    time.sleep(2)

    except:
        prYellow("\r\n" + "Going back to the previous menu.")
        time.sleep(2)

def dataset_list_mt(params):

    server_id = params[0]
    access_token = params[1]
    cvl = params[2]
    i = params[3]


    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }
    rows = requests.get('https://{}.salesforce.com'.format(server_id) + '{}'.format(cvl), headers=headers)
    rows_json = json.loads(rows.text)
    rows_json = rows_json.get('totalRows')
    rows_count = int(rows_json)
    return rows_count
