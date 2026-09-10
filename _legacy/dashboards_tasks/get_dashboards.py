import json, requests, time
from misc_tasks.terminal_colors import *
from misc_tasks.sfdc_login import *
from dashboards_tasks.get_dash_datasets import *
from dashboards_tasks.backup_dash_json import *
from dashboards_tasks.get_dashboard_history import *
from dashboards_tasks.delete_dashboard import *
from misc_tasks.line import *

def get_dashboards_main(access_token,server_id,server_domain):
    try:
        run_token = True
        check_token = "Y"
        while run_token:
            prGreen("\r\n" + "What do you want to do?:")
            time.sleep(0.15)
            prYellow("(Choose a number from the list below)" + "\r\n")
            time.sleep(0.15)
            prCyan("1 - List the 50 most recent used dashboards")
            time.sleep(0.05)
            prCyan("2 - Search a dashboard by ID/Name/Label")
            time.sleep(0.05)

            user_input_ = input("\r\n" + "Enter your selection: ")
            line_print()

            if user_input_ == "1":

                prGreen("\r\nRetrieving dashboards...")
                line_print()
                headers = {
                    'Authorization': "Bearer {}".format(access_token)
                    }
                resp = requests.get('https://{}.my.salesforce.com/services/data/v53.0/wave/dashboards?sort=Mru&pageSize=50'.format(server_domain), headers=headers)

                formatted_response = json.loads(resp.text)
                formatted_response_str = json.dumps(formatted_response, indent=2)
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
                line_print()

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
                            time.sleep(0.15)
                            prYellow("(Choose a number from the list below)" + "\r\n")
                            time.sleep(0.15)
                            prCyan("1 - List Dashboard Datasets")
                            time.sleep(0.15)
                            prCyan("2 - Show Version History / Restore")
                            time.sleep(0.15)
                            prRed("3 - Delete Dashboard")
                            line_print()

                            user_input = input("\r\n" + "Enter your selection: ")

                            if user_input == "1":
                                get_dash_datasets(access_token,dashboard_,server_id,server_domain)

                            if user_input == "2":
                                dashboard_history(access_token,dashboard_,server_id,historiesUrl,server_domain)

                            #if user_input == "3":
                            #    backup_dash_json(access_token,dashboard_,server_id,dashboard_label,server_domain)

                            if user_input == "3":
                                delete_dash(access_token,dashboard_,server_id,server_domain)

                            prCyan("\r\n" + "Selected Dashboard: {} - {}".format(dashboard_, dashboard_label))
                            check_token = input("Press \"Y\" to see the dashboard actions or hit any key to go back" + "\r\n")

                            if check_token == "Y" or check_token == "y":
                                run_token = True
                            else:
                                run_token = False

                except ValueError:
                    prYellow("\r\n" + "Going back to the previous menu.")
                    time.sleep(2)

            if user_input_ == "2":

                q_string = input("Enter the search term you want to use (ID/Name/Label without quotes and space separated): ")
                prGreen("\r\nRetrieving dashboards...")
                line_print()
                headers = {
                    'Authorization': "Bearer {}".format(access_token)
                    }
                resp = requests.get('https://{}.my.salesforce.com/services/data/v53.0/wave/dashboards?q={}&sort=Mru&pageSize=200'.format(server_domain,q_string), headers=headers)

                formatted_response = json.loads(resp.text)
                formatted_response_str = json.dumps(formatted_response, indent=2)
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
                line_print()

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
                            time.sleep(0.15)
                            prYellow("(Choose a number from the list below)" + "\r\n")
                            time.sleep(0.15)
                            prCyan("1 - List Dashboard Datasets")
                            time.sleep(0.15)
                            prCyan("2 - Show Version History / Restore")
                            time.sleep(0.15)
                            prCyan("3 - Backup JSON definition")
                            time.sleep(0.15)
                            prRed("4 - Delete Dashboard")
                            time.sleep(0.5)
                            line_print()

                            user_input = input("\r\n" + "Enter your selection: ")

                            if user_input == "1":
                                get_dash_datasets(access_token,dashboard_,server_id,server_domain)

                            if user_input == "2":
                                dashboard_history(access_token,dashboard_,server_id,historiesUrl,server_domain)

                            if user_input == "3":
                                backup_dash_json(access_token,dashboard_,server_id,dashboard_label,server_domain)

                            if user_input == "4":
                                delete_dash(access_token,dashboard_,server_id,server_domain)

                            prCyan("\r\n" + "Selected Dashboard: {} - {}".format(dashboard_, dashboard_label))
                            check_token = input("Press \"Y\" to see the dashboard actions or hit any key to go back" + "\r\n")

                            if check_token == "Y" or check_token == "y":
                                run_token = True
                            else:
                                run_token = False


                except ValueError:
                    prYellow("\r\n" + "Going back to the previous menu.")
                    time.sleep(2)

            if check_token == "Y" or check_token == "y":
                run_token = True
            else:
                run_token = False
                prYellow("\r\n" + "Going back to the previous menu.")
                time.sleep(0.5)
    except:
        traceback.print_exc()
        prYellow("\r\n" + "Going back to the previous menu.")
        time.sleep(1)
