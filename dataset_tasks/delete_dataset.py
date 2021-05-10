import json
import requests
from terminal_colors import *
from sfdc_login import *
import time

def delete_dataset(access_token,dataset_,server_id):

    prRed("\r\n" + "This will permanently delete your dataset.")
    time.sleep(5)
    user_input = input("Do you want to proceed? (Y/N): ")

    try:
            if user_input == "Y" or user_input == "y":
                headers = {
                    'Authorization': "Bearer {}".format(access_token)
                    }
                resp = requests.delete('https://{}.salesforce.com/services/data/v51.0/wave/datasets/{}'.format(server_id,dataset_), headers=headers)

                formatted_response = json.loads(resp.text)
                formatted_response_str = json.dumps(formatted_response, indent=2)
                prGreen(formatted_response_str)
                prYellow("\r\n" + "The dataset has been deleted.")
            elif user_input == "N" or user_input == "n":
                prYellow("\r\n" + "Dataset deletion cancelled.")
                time.sleep(2)
            else:
                print("\r\n" + "Wrong value entered. Going back.")
                time.sleep(2)
    except:
        pass
