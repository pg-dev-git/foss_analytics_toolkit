import json, requests, time
from misc_tasks.terminal_colors import *
from misc_tasks.sfdc_login import *

def delete_dataset(access_token,dataset_,server_id,server_domain):

    prRed("\r\n" + "This will permanently delete your dataset.")
    time.sleep(5)
    user_input = input("Do you want to proceed? (Y/N): ")

    try:
            if user_input == "Y" or user_input == "y":
                headers = {
                    'Authorization': "Bearer {}".format(access_token)
                    }
                resp = requests.delete('https://{}.my.salesforce.com/services/data/v53.0/wave/datasets/{}'.format(server_domain,dataset_), headers=headers)
                print(resp.text)

                #formatted_response = json.loads(resp.text)
                #formatted_response_str = json.dumps(formatted_response, indent=2)
                #prGreen(formatted_response_str)
                prYellow("\r\n" + "The dataset has been deleted.")
            elif user_input == "N" or user_input == "n":
                prYellow("\r\n" + "Dataset deletion cancelled.")
                time.sleep(2)
            else:
                print("\r\n" + "Wrong value entered. Going back.")
                time.sleep(2)
    except:
        traceback.print_exc()
        pass
