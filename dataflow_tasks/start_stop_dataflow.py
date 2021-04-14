import json
import requests
from terminal_colors import *
from sfdc_login import *
import os

d_job_id = "ffghd234dffhd23"

def start_dataflow(access_token,dataflow_id_):

        headers = {
            'Authorization': "Bearer {}".format(access_token),
            'Content-Type': "application/json"
            }

        payload = {"dataflowId": "{}".format(dataflow_id_),"command": "start"}

        payload = json.dumps(payload)

        resp = requests.post('https://na156.salesforce.com/services/data/v51.0/wave/dataflowjobs', headers=headers, data=payload)

        formatted_response = json.loads(resp.text)
        formatted_response_str = json.dumps(formatted_response, indent=2)
        prGreen(formatted_response_str)
        global d_job_id
        d_job_id = formatted_response.get("id")
        print(d_job_id)

        prGreen("\r\n" + "Dataflow started. Check the Data Manager for more details." + "\r\n")

def stop_dataflow(access_token,dataflow_id_):

        headers = {
            'Authorization': "Bearer {}".format(access_token),
            'Content-Type': "application/json"
            }

        payload = {"command": "stop"}

        payload = json.dumps(payload)

        print(d_job_id)

        resp = requests.patch('https://na156.salesforce.com/services/data/v51.0/wave/dataflowjobs/{}'.format(d_job_id), headers=headers, data=payload)

        prGreen("\r\n" + "Dataflow stopped. Check the Data Manager for more details." + "\r\n")
