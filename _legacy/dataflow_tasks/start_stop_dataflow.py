import json, requests, os, time
from misc_tasks.terminal_colors import *
from misc_tasks.sfdc_login import *
from misc_tasks.line import *

d_job_id = "ffghd234dffhd23"

def start_dataflow(access_token,dataflow_id_,server_id,dataflow_name_,server_domain):

        headers = {
            'Authorization': "Bearer {}".format(access_token),
            'Content-Type': "application/json"
            }

        payload = {"dataflowId": "{}".format(dataflow_id_),"command": "start"}

        payload = json.dumps(payload)

        resp = requests.post('https://{}.my.salesforce.com/services/data/v53.0/wave/dataflowjobs'.format(server_domain), headers=headers, data=payload)

        formatted_response = json.loads(resp.text)
        formatted_response_str = json.dumps(formatted_response, indent=2)
        global d_job_id
        d_job_id = formatted_response.get("id")

        prGreen("\r\n" + "Dataflow started. Check the Data Manager for more details." + "\r\n")
        line_print()

        time.sleep(1)

        prYellow("\r\n" + "Dataflow selected: {} - {}".format(dataflow_name_, dataflow_id_) + "\r\n")

def stop_dataflow(access_token,dataflow_id_,server_id,dataflow_name_,server_domain):

        headers = {
            'Authorization': "Bearer {}".format(access_token),
            'Content-Type': "application/json"
            }

        payload = {"command": "stop"}

        payload = json.dumps(payload)

        print(d_job_id)

        resp = requests.patch('https://{}.my.salesforce.com/services/data/v53.0/wave/dataflowjobs/{}'.format(server_domain,d_job_id), headers=headers, data=payload)

        prGreen("\r\n" + "Dataflow stopped. Check the Data Manager for more details." + "\r\n")

        line_print()

        time.sleep(2)

        prYellow("\r\n" + "Dataflow selected: {} - {}".format(dataflow_name_, dataflow_id_) + "\r\n")
