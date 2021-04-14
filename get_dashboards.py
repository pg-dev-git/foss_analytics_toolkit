import json
import requests
from terminal_colors import *
from sfdc_login import *

def get_dashboards(access_token):
    prGreen("\r\n" + "getting dashboards list..." + "\r\n")
    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }
    resp = requests.get('https://na91.salesforce.com/services/data/v51.0/wave/dashboards', headers=headers)
    #print(resp.json())
    #Print PrettyJSON in Terminal

    formatted_response = json.loads(resp.text)
    #print(formatted_response)
    #formatted_response_str = json.dumps(formatted_response, indent=2
    #prGreen(formatted_response_str)

    dashboards_list = formatted_response.get('dashboards')

    counter = 0

    for x in dashboards_list:
        counter += 1
        print("{} - ".format(counter) ,"Datset id: ",x["id"]," - Label: ",x["label"])
    print("\r\n")
