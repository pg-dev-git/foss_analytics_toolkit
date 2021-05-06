import time
import json
import requests
from terminal_colors import *
from sfdc_login import *
import math
import csv
import pandas as pd
import os
import base64
from dataset_tasks.json_metadata_generator import *
import math

def append_csv_dataset(access_token,dataset_name_,dataset_,server_id,dataset_name):

    try:
        dataset_upload_dir = "dataset_upload"
        os.mkdir(dataset_upload_dir)
    except OSError as error:
            print(" ")

    cd = os.getcwd()
    #print(cd)

    d_ext = "{}".format(cd)+"\\dataset_upload\\"
    #print(d_ext)

    os.chdir(d_ext)

    user_input_1 = "Xhhrydjanshtttx"
    user_input_2 = "xbyr5546shdnc"
    user_input_3 = 9567385638567265

    #Input check for file placement
    while user_input_1 == "Xhhrydjanshtttx" or user_input_1 == "N" or user_input_1 == "n":
        user_input_1 = input("\r\n" + "Have you placed the CSV file in the \'dataset_upload\' folder? (Y/N): ")
        time.sleep(2)
        if user_input_1 == "Y" or user_input_1 == "y":
            print("\r\n")
        elif user_input_1 == "N" or user_input_1 == "n":
            prYellow("Please place the file and try again.")
            time.sleep(1)
        else:
            prRed("Wrong value. Try again.")
            time.sleep(1)

    #Input check for file encoding
    while user_input_2 == "xbyr5546shdnc" or user_input_2 == "N" or user_input_2 == "n":
        user_input_2 = input("\r\n" + "Is the CSV file comma separated and UTF-8 encoded? (Y/N): ")
        time.sleep(2)
        if user_input_2 == "Y" or user_input_2 == "y":
            print("")
            time.sleep(0.5)
        elif user_input_2 == "N" or user_input_2 == "n":
            prYellow("Please save your file as comma separated (not tab or semicolon) and ensure it's UTF-8 encoded.")
            time.sleep(2)
        else:
            prRed("Wrong value. Try again.")
            time.sleep(1)

    #Input check for total # of rows
    while user_input_3 == 9567385638567265 or type(user_input_3) != int or user_input_3 < 1:
        user_input_3 = input("\r\n" + "What's the total row count in your file? (integer): ")
        time.sleep(2)
        try:
            user_input_3 = int(user_input_3)
            if type(user_input_3) == int and user_input_3 > 0:
                print("")
                time.sleep(0.5)
            elif type(user_input_3) == int and user_input_3 < 1:
                prYellow("\r\n" + "Did you enter the right number of rows? Try again.")
                time.sleep(2)
            else:
                prRed("\r\n" + "Please use an integer.")
                time.sleep(1)
        except ValueError:
            prRed("\r\n" + "Please use an integer.")
            time.sleep(2)

    if (user_input_1 == "Y" or user_input_1 == "y") and (user_input_2 == "Y" or user_input_2 == "y"):
        dataset_name = input("Enter your filename without the csv extension:")
        time.sleep(2)
        prGreen("\r\n" + "Locally generating json metadata from the csv file and encoding it to base64.")
        time.sleep(2)
        _start = time.time()
        csv_upload_json_meta(dataset_name_,dataset_name)
        meta_json_data = open("{}_CSV_upload_metadata.json".format(dataset_name), 'rb').read()
        meta_json_base64_encoded = base64.b64encode(meta_json_data).decode('UTF-8')
        #os.remove("{}_CSV_upload_metadata.json".format(dataset_name))
        _end = time.time()
        enc_time = round((_end-_start),2)
        prGreen("\r\n" + "Task Finished in {}s".format(enc_time))
        time.sleep(2)

        batches_ = math.ceil(user_input_3 / 50000)

        batch_count = 0

        skiprows = 0

        operation_flag = 'Append'

        if batches_ > 0:
            prGreen("\r\n" + "Your file will be upladed in {} batches".format(batches_))

            for x in range(batches_):

                batch_count += 1

                prCyan("\r\n" + "*** Starting batch #{} ***".format(batch_count))

                load_csv_split = pd.read_csv("{}.csv".format(dataset_name), skiprows=skiprows, nrows=50000, chunksize=50000)

                export_csv = pd.concat(load_csv_split)
                export_csv = export_csv.to_csv(r"{}_dataset_split_{}.csv".format(dataset_name,batch_count), index = None, header=True, encoding='utf-8-sig')
                skiprows += 50000

                prGreen("\r\n" + "Locally encoding csv batch #{} to base64.".format(batch_count))
                time.sleep(2)
                _start = time.time()
                #Enconde csv to base64 for upload - start
                data = open("{}_dataset_split_{}.csv".format(dataset_name,batch_count), 'rb').read()
                base64_encoded = base64.b64encode(data).decode('UTF-8')
                #Enconde csv to base64 for upload - end
                _end = time.time()
                enc_time = round((_end-_start),2)
                prGreen("\r\n" + "CSV encoded in {}s".format(enc_time))
                time.sleep(2)

                headers = {'Authorization': "Bearer {}".format(access_token),
                           'Content-Type': "application/json"}

                payload = {'Format' : 'Csv','EdgemartAlias' : '{}'.format(dataset_name_),'Operation': '{}'.format(operation_flag),'Action': 'None','MetadataJson': "{}".format(meta_json_base64_encoded)}
                payload = json.dumps(payload)
                prGreen("\r\n" + "Creating Workbench Job #{} of {}".format(batch_count,batches_))

                x = 0
                while x != 1:
                    try:
                        resp = requests.post('https://{}.salesforce.com/services/data/v51.0/sobjects/InsightsExternalData'.format(server_id), headers=headers, data=payload)
                        time.sleep(1)
                        resp_results = json.loads(resp.text)
                        success = resp_results.get('success')
                        errors = resp_results.get('errors')
                        formatted_response_str = json.dumps(resp_results, indent=2)
                        #prYellow(formatted_response_str)
                        job_id = resp_results.get("id")
                        prGreen("\r\n" + "Workbench Job Id: {}".format(job_id))
                        if success:
                            prYellow("Status: Succesfull")
                            x += 1
                        else:
                            prRed(errors)
                            time.sleep(1)
                    except:
                        pass

                payload = {'DataFile' : '{}'.format(base64_encoded),'InsightsExternalDataId' : '{}'.format(job_id),'PartNumber': 1}
                payload = json.dumps(payload)
                prGreen("\r\n" + "Uploading file to TCRM")
                time.sleep(1)

                x = 0
                while x != 1:
                    try:
                        _start = time.time()
                        resp = requests.post('https://{}.salesforce.com/services/data/v51.0/sobjects/InsightsExternalDataPart'.format(server_id), headers=headers, data=payload)
                        resp_results = json.loads(resp.text)
                        formatted_response_str = json.dumps(resp_results, indent=2)
                        #prYellow(formatted_response_str)
                        success = resp_results.get('success')
                        errors = resp_results.get('errors')
                        _end = time.time()
                        upl_time = round((_end-_start),2)
                        prGreen("\r\n" + "CSV uploaded in {}s".format(upl_time))
                        time.sleep(1)
                        if success:
                            prYellow("Status: Succesfull")
                            x += 1
                        else:
                            prRed(errors)
                            time.sleep(1)
                    except:
                        pass

                payload = {'Action' : 'Process'}
                payload = json.dumps(payload)

                x = 0
                while x != 1:
                    try:
                        resp = requests.patch('https://{}.salesforce.com/services/data/v51.0/sobjects/InsightsExternalData/{}'.format(server_id,job_id), headers=headers, data=payload)
                        prGreen("\r\n" + "Batch #{} completed.".format(batch_count))
                        prYellow("TCRM Data Manager Job triggered. Check the data manager for more details." + "\r\n")
                        x += 1
                        time.sleep(2)
                    except:
                        pass

                if os.path.exists("{}_dataset_split_{}.csv".format(dataset_name,batch_count)):
                    os.remove("{}_dataset_split_{}.csv".format(dataset_name,batch_count))

                operation_flag = 'Append'

    #Go back to parent folder:
    os.chdir("..")
    prCyan("\r\n" + "Dataset selected: {} - {}".format(dataset_name, dataset_))
