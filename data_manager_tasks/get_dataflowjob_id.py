import json
import requests
import time
import pandas as pd

access_token = "00D0q0000009l1O!AQ8AQON0OKHHj.ZUACTn1E1oyFYCM46ZwP160_nbyV6CC4YojamJJNMREWX8GhkaS_iW0gjvtIDgT8ZtPIhj75m0RnXLBOPo"
server_domain = "ciscosales--mon"

def examine_dataflow(access_token,server_domain,flow_id):

    pd_list = []
    df = pd.DataFrame(pd_list, columns = ['Node Name' , 'Duration', 'Input Rows' , 'Output Rows', 'Node Type', 'Status'])

    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }
    resp = requests.get('https://{}.my.salesforce.com/services/data/v53.0/wave/dataflowjobs/{}/nodes'.format(server_domain,flow_id), headers=headers)

    formatted_response = json.loads(resp.text)
    #print(formatted_response)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    nodes_list = formatted_response.get("nodes")

    counter = 0
    for x in nodes_list:
        counter += 1
        try:
            in_rows = (x["inputRows"])
            if in_rows.get("processedCount") != None:
                in_rows = (in_rows.get("processedCount"))
            else:
                in_rows = 0
        except:
            pass

        try:
            out_rows = (x["outputRows"])
            if out_rows.get("processedCount") != None:
                out_rows = (out_rows.get("processedCount"))
            else:
                out_rows = 0
        except:
            pass

        pd_list.append({"Node Name": x["name"],
                         "Duration (s)": x["duration"],
                         "Input Rows": in_rows,
                         "Output Rows": out_rows,
                         "Node Type": x["nodeType"],
                         "Status": x["status"]})

    df = pd.DataFrame(pd_list, columns = ['Node Name' , 'Duration (s)', 'Input Rows' , 'Output Rows', 'Node Type', 'Status'])
    print(df)
