import json, requests, time, queue, threading
from terminal_colors import *
from sfdc_login import *
from dataset_tasks.get_dataset_field_detail import *
from dataset_tasks.get_dataset_extract_MP import *
from dataset_tasks.upload_dataset import *
from dataset_tasks.dataset_backup_user_xmd import *
from dataset_tasks.xmd_cleanup import *
from dataset_tasks.append_dataset import *
from dataset_tasks.delete_dataset import *
from dataset_tasks.get_dataset_history import *
from dataset_tasks.get_dataset_dependencies import *
from line import *

def get_platform():
    platforms = {
        'linux1' : 'Linux',
        'linux2' : 'Linux',
        'darwin' : 'OS X',
        'win32' : 'Windows'
    }
    if sys.platform not in platforms:
        return sys.platform

    return platforms[sys.platform]

def get_datasets(access_token,server_id,server_domain):
    try:
        run_token = True
        while run_token:
            prGreen("\r\n" + "What do you want to do?:")
            time.sleep(0.15)
            prYellow("(Choose a number from the list below)" + "\r\n")
            time.sleep(0.15)
            prCyan("1 - List the 50 most recent used datasets")
            time.sleep(0.05)
            prCyan("2 - Search a dataset by ID/Name/Label")
            time.sleep(0.05)

            user_input_ = input("\r\n" + "Enter your selection: ")
            line_print()

            if user_input_ == "1":
                prGreen("\r\nRetrieving datasets...")
                line_print()
                headers = {
                    'Authorization': "Bearer {}".format(access_token)
                    }
                resp = requests.get('https://{}.my.salesforce.com/services/data/v53.0/wave/datasets?sort=Mru&pageSize=51'.format(server_domain), headers=headers)
                #print(resp.json())

                formatted_response = json.loads(resp.text)
                #print(formatted_response)
                formatted_response_str = json.dumps(formatted_response, indent=2)
                #prGreen(formatted_response_str)



                datasets_list = formatted_response.get('datasets')
                next_page = formatted_response.get('nextPageUrl')
                #prGreen(datasets_list)

                counter = 0
                counterx = 0

                for xx in datasets_list:
                    counterx += 1

                i = 0

                que = queue.Queue()
                threads = list()

                t_result = []

                for index in range(counterx):
                    try:
                        cvl = datasets_list[index]["currentVersionUrl"]
                        params = [server_id,server_domain,access_token,cvl,i]
                        x = threading.Thread(target=lambda q, arg1: q.put(dataset_list_mt(arg1)), args=(que,params))
                        threads.append(x)
                        x.start()
                        time.sleep(0.15)
                    except:
                        time.sleep(0.05)
                        index -= 1
                        counterx -= 1


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

                        dataset_rows = t_result[action_track-1]

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
                            prCyan("8 - XMD Cleanup (Windows only)")
                            time.sleep(0.15)
                            prRed("9 - Delete dataset")
                            time.sleep(0.5)

                            user_input = input("\r\n" + "Enter your selection: ")
                            line_print()

                            if user_input == "1":
                                get_datasets_field_details(access_token,dataset_,server_id,dataset_name,server_domain)

                            if user_input == "2":
                                #get_datasets_extract(access_token,dataset_,server_id,dataset_name)
                                get_datasets_extract_mp(access_token,dataset_,server_id,dataset_rows,server_domain)

                            if user_input == "3":
                                upload_csv_dataset(access_token,dataset_name,dataset_,server_id,dataset_name,server_domain)

                            if user_input == "4":
                                append_csv_dataset(access_token,dataset_name,dataset_,server_id,dataset_name,server_domain)

                            if user_input == "5":
                                backup_xmd_user(access_token,dataset_,server_id,dataset_name,server_domain)

                            if user_input == "6":
                                dataset_history(access_token,dataset_,server_id,versionsUrl,dataset_name,server_domain)

                            if user_input == "7":
                                dataset_dependencies(access_token,dataset_,server_id,dataset_name,server_domain)

                            if user_input == "8":
                                xmd_cleanup(access_token,dataset_,server_id,dataset_name,server_domain)

                            if user_input == "9":
                                delete_dataset(access_token,dataset_,server_id,server_domain)

                            check_token = input("\r\nPress \"Y\" to see the dataset actions or hit any key to go back")

                            line_print()

                            if check_token == "Y" or check_token == "y":
                                run_token = True
                            else:
                                run_token = False
                                #prYellow("\r\n" + "Going back to the previous menu.")
                                #time.sleep(2)

                except:
                    #traceback.print_exc()
                    #prYellow("\r\n" + "Going back to the previous menu.")
                    check_token = False
                    #time.sleep(0.5)

            if user_input_ == "2":
                #get_datasets_extract(access_token,dataset_,server_id,dataset_name)
                q_string = input("Enter the search term you want to use (ID/Name/Label without quotes and space separated): ")
                prGreen("\r\nSearching datasets...")
                line_print()
                headers = {
                    'Authorization': "Bearer {}".format(access_token)
                    }
                resp = requests.get('https://{}.my.salesforce.com/services/data/v53.0/wave/datasets?q={}&sort=Mru&pageSize=200'.format(server_domain,q_string), headers=headers)
                #print(resp.json())

                formatted_response = json.loads(resp.text)
                #print(formatted_response)
                formatted_response_str = json.dumps(formatted_response, indent=2)
                #prGreen(formatted_response_str)



                datasets_list = formatted_response.get('datasets')
                next_page = formatted_response.get('nextPageUrl')
                #prGreen(datasets_list)

                counter = 0
                counterx = 0

                for xx in datasets_list:
                    counterx += 1

                i = 0

                que = queue.Queue()
                threads = list()

                t_result = []

                for index in range(counterx):
                    try:
                        cvl = datasets_list[index]["currentVersionUrl"]
                        params = [server_id,server_domain,access_token,cvl,i]
                        x = threading.Thread(target=lambda q, arg1: q.put(dataset_list_mt(arg1)), args=(que,params))
                        threads.append(x)
                        x.start()
                        time.sleep(0.15)
                    except:
                        time.sleep(0.05)
                        index -= 1
                        counterx -= 1


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

                        dataset_rows = t_result[action_track-1]

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
                                get_datasets_field_details(access_token,dataset_,server_id,dataset_name,server_domain)

                            if user_input == "2":
                                #get_datasets_extract(access_token,dataset_,server_id,dataset_name)
                                get_datasets_extract_mp(access_token,dataset_,server_id,dataset_rows,server_domain)

                            if user_input == "3":
                                upload_csv_dataset(access_token,dataset_name,dataset_,server_id,dataset_name,server_domain)

                            if user_input == "4":
                                append_csv_dataset(access_token,dataset_name,dataset_,server_id,dataset_name,server_domain)

                            if user_input == "5":
                                backup_xmd_user(access_token,dataset_,server_id,dataset_name)

                            if user_input == "6":
                                dataset_history(access_token,dataset_,server_id,versionsUrl,dataset_name)

                            if user_input == "7":
                                dataset_dependencies(access_token,dataset_,server_id,dataset_name)

                            if user_input == "8":
                                xmd_cleanup(access_token,dataset_,server_id,dataset_name)

                            if user_input == "9":
                                delete_dataset(access_token,dataset_,server_id,server_domain)

                            check_token = input("\r\nPress \"Y\" to see the dataset actions or hit any key to go back")

                            line_print()

                            if check_token == "Y" or check_token == "y":
                                run_token = True
                            else:
                                run_token = False
                                #prYellow("\r\n" + "Going back to the previous menu.")
                                #time.sleep(0.5)

                except:
                    #traceback.print_exc()
                    #prYellow("\r\n" + "Going back to the previous menu.")
                    check_token = False
                    #time.sleep(0.5)

            line_print()

            if check_token == "Y" or check_token == "y":
                run_token = True
            else:
                run_token = False
                prYellow("\r\n" + "Going back to the previous menu.")
                time.sleep(0.5)

    except:
        traceback.print_exc()
        prYellow("\r\n" + "Going back to the previous menu.")
        time.sleep(2)




def dataset_list_mt(params):

    server_id = params[0]
    server_domain = params[1]
    access_token = params[2]
    cvl = params[3]
    i = params[4]


    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }
    rows = requests.get('https://{}.my.salesforce.com'.format(server_domain) + '{}'.format(cvl), headers=headers)
    rows_json = json.loads(rows.text)
    try:
        rows_json = rows_json.get('totalRowCount')
    except:
        rows_json = "0"
    rows_count = int(rows_json)
    return rows_count
