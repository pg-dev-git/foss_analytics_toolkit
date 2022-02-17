import json
import requests
from terminal_colors import *
import time
from line import *

def get_EA_limits(access_token,server_id):

    time.sleep(0.5)
    prGreen("\r\n" + "Checking the instance..." + "\r\n")
    line_print()
    headers = {
        'Authorization': "Bearer {}".format(access_token,server_id)
        }
    resp = requests.get('https://{}.salesforce.com/services/data/v53.0/wave/limits'.format(server_id), headers=headers)
    #print(resp.json())
    #Print PrettyJSON in Terminal

    formatted_response = json.loads(resp.text)
    #print(formatted_response)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    #prGreen(formatted_response_str)

    limits = formatted_response.get('limits')

    current = int(limits[0]['current'])
    max_ = int(limits[0]['max'])
    used_ = round((current / max_)*100,2)
    diff_ = int(100 - used_)

    prCyan("\r\nTotal Rows Available: {}".format(max_))

    if used_ < 50:
        prGreen("\r\nTotal Rows in Use: {} - {}%".format(current,used_))
        time.sleep(1)
    elif used_ >= 50 and used_ < 80:
        prYellow("\r\nTotal Rows in Use: {} - {}%".format(current,used_))
        time.sleep(1)
    elif used_ >= 80 and used_ < 90:
        prRed("\r\nTotal Rows in Use: {} - {}%".format(current,used_))
        time.sleep(1)
    elif used_ >= 90 and used_ < 100:
        prRed("\r\nTotal Rows in Use: {} - {}%".format(current,used_))
        prYellow("There is only {}% left of rows available in your instance!".format(diff_))
        time.sleep(2)
    else:
        prRed("{} - {}%".format(current,used_))
        prYellow("You have used all rows available in your instance. You won't be able to create new datasets!")
        time.sleep(3)
