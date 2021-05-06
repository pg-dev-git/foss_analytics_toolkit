import json
import requests
from terminal_colors import *
from sfdc_login import *
import time

def dataset_dependencies(access_token,dataset_,server_id,dataset_name):

    prGreen("\r\n" + "Getting dependencies..." + "\r\n")
    time.sleep(2)

    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }
    resp = requests.get('https://{}.salesforce.com/services/data/v51.0/wave/dependencies/{}'.format(server_id,dataset_), headers=headers)
    #print(resp.json())
    #Print PrettyJSON in Terminal

    formatted_response = json.loads(resp.text)
    #print(formatted_response)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    prGreen(formatted_response_str)

    counter = 0

    try:
        try:
            depend_dash_list = formatted_response.get('dashboards').get("dependencies")

            for x in depend_dash_list:
                counter += 1
                if counter >= 1 and counter <= 9:
                    print(" {} - ".format(counter) ,"Dashboard id: ",x["id"]," - Type: ",x["type"]," - Name: ",x["name"])
                else:
                    print("{} - ".format(counter) ,"Dashboard id: ",x["id"]," - Type: ",x["type"]," - Name: ",x["name"])
            #print("\r\n")
        except AttributeError:
            pass

        try:
            depend_data_list = formatted_response.get('datasets').get("dependencies")

            for x in depend_data_list:
                counter += 1
                if counter >= 1 and counter <= 9:
                    print(" {} - ".format(counter) ,"Dataset id: ",x["id"]," - Type: ",x["type"]," - Name: ",x["name"])
                else:
                    print("{} - ".format(counter) ,"Dataset id: ",x["id"]," - Type: ",x["type"]," - Name: ",x["name"])
            #print("\r\n")
        except AttributeError:
            pass

        try:
            depend_lens_list = formatted_response.get('lenses').get("dependencies")
            for x in depend_lens_list:
                counter += 1
                if counter >= 1 and counter <= 9:
                    print(" {} - ".format(counter) ,"     Lens id: ",x["id"]," - Type: ",x["type"],"      - Name: ",x["name"])
                else:
                    print("{} - ".format(counter) ,"     Lens id: ",x["id"]," - Type: ",x["type"],"      - Name: ",x["name"])
            #print("\r\n")
        except AttributeError:
            pass

        try:
            depend_flow_list = formatted_response.get('workflows').get("dependencies")
            for x in depend_flow_list:
                counter += 1
                if counter >= 1 and counter <= 9:
                    print(" {} - ".format(counter) ," Dataflow id: ",x["id"]," - Type: ",x["type"],"  - Name: ",x["name"])
                else:
                    print("{} - ".format(counter) ," Dataflow id: ",x["id"]," - Type: ",x["type"],"  - Name: ",x["name"])
            #print("\r\n")
        except AttributeError:
            pass

    except AttributeError:
        prYellow("\r\n" + "This asset doesn't have any dependencies.")

    if counter == 0:
        prYellow("\r\n" + "This asset doesn't have any dependencies.")

    prCyan("\r\n" + "Dataset selected: {} - {}".format(dataset_name, dataset_))
