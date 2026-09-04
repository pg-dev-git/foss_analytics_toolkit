import json, requests, os, time
from misc_tasks.terminal_colors import *
from misc_tasks.sfdc_login import *
from misc_tasks.line import *

def dataset_history(access_token,dataset_,server_id,versionsUrl,dataset_name,server_domain):
    headers = {
        'Authorization': "Bearer {}".format(access_token),
        'Content-Type': "application/json"
        }
    resp = requests.get('https://{}.my.salesforce.com{}'.format(server_domain,versionsUrl), headers=headers)

    formatted_response = json.loads(resp.text)
    formatted_response_str = json.dumps(formatted_response, indent=2)

    dataset_his_list = formatted_response.get('versions')

    #Check if there are available histories to backup - start:

    check_counter = 0

    for x in dataset_his_list:
        check_counter += 1

    #Check if there are available histories to backup - end.

    if check_counter != 0:

        counter_2 = 0

        counter = 0

        prGreen("\r\nGetting all versions available...")
        line_print()

        for x in dataset_his_list:
            counter += 1
            print("{} -".format(counter),"Id:",x["id"],"- Rows:",x["totalRowCount"],"- Created On:",x["createdDate"],"- Last Modified On:",x["lastModifiedDate"])

        line_print()
        prYellow("\r\n#1 is the latest version of the Dataset.")
        line_print()

        ####action_track = input("Choose a Dataflow History id between #2 and {} to replace the current version or hit any other key to go back:".format(counter))

        counter_2 = 0

    else:
        prRed("\r\n" + "There are no history records available." + "\r\n")
        line_print()

    prCyan("\r\n" + "Dataset selected: {} - {}".format(dataset_name, dataset_))
