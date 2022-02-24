import json
import requests
from terminal_colors import *
from sfdc_login import *
import time

def delete_dash(access_token,dashboard_,server_id,server_domain):

    prRed("\r\n" + "This will permanently delete your Dashboard.")
    time.sleep(3)
    user_input = input("Do you want to proceed? (Y/N): ")

    try:
            if user_input == "Y" or user_input == "y":
                time.sleep(2)
                headers = {
                    'Authorization': "Bearer {}".format(access_token)
                    }
                resp = requests.delete('https://{}.my.salesforce.com/services/data/v53.0/wave/dashboards/{}'.format(server_domain,dashboard_), headers=headers)

                try:
                    formatted_response = json.loads(resp.text)
                    #formatted_response_str = json.dumps(formatted_response, indent=2)
                    #prGreen(formatted_response_str)
                    #print(formatted_response)
                    message = formatted_response[0]['message']
                    prYellow("\r\n" + "{}".format(message))
                    time.sleep(2)
                except:
                    prYellow("\r\n" + "The Dashboard has been deleted.")
                    time.sleep(2)
            elif user_input == "N" or user_input == "n":
                prYellow("\r\n" + "Dataset deletion cancelled.")
                time.sleep(2)
            else:
                print("\r\n" + "Wrong value entered. Going back.")
                time.sleep(2)
    except:
        pass
