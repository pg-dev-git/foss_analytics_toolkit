import json, requests, math, csv, glob, os, base64, threading, time, pandas as pd
from misc_tasks.terminal_colors import *
from misc_tasks.sfdc_login import *
from dataset_tasks.dataset_extract_MT import *

#os.chdir("/Users/pgagliar/Desktop/api_test/")

def get_datasets_extract(access_token,dataset_,server_id):

    try:
        dataset_extraction_dir = "dataset_extraction"
        os.mkdir(dataset_extraction_dir)
    except OSError as error:
            print(" ")

    cd = os.getcwd()
    #print(cd)

    os_ = sfdc_login.get_platform()

    if os_ == "Windows":
        d_ext = "{}".format(cd)+"\\dataset_extraction\\"
    else:
        d_ext = "{}".format(cd)+"/dataset_extraction/"

    #print(d_ext)

    os.chdir(d_ext)

    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }
    resp = requests.get('https://{}.salesforce.com/services/data/v53.0/wave/datasets/{}'.format(server_id,dataset_), headers=headers)

    formatted_response = json.loads(resp.text)
    #print(formatted_response)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    #prGreen(formatted_response_str)


    dataset_current_version_url = formatted_response.get('currentVersionUrl')
    dataset_currentVersionId = formatted_response.get('currentVersionId')
    dataset_name = formatted_response.get('name')

    saql = "q = load \"{}/{}\";q = group  q by all;q = foreach q generate count() as 'count';q = limit q 1;".format(dataset_,dataset_currentVersionId)

    saql_payload = {"name": "get_rows","query": str(saql), "queryLanguage": "SAQL"}

    saql_payload = json.dumps(saql_payload)

    headers = {'Authorization': "Bearer {}".format(access_token),
               'Content-Type': "application/json"
               }

    resp = requests.post('https://{}.salesforce.com/services/data/v53.0/wave/query'.format(server_id), headers=headers, data=saql_payload)
    query_results = json.loads(resp.text)
    count_rows = query_results.get('results')
    count_rows = count_rows['records']
    count_rows = count_rows[0]
    count_rows = count_rows.get('count')
    batches_ = math.ceil(count_rows / 9999)
    #print(batches_)

    dataset_current_version_url = "https://{}.salesforce.com".format(server_id) + "{}".format(dataset_current_version_url) + "/xmds/main"

    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }

    resp = requests.get('{}'.format(dataset_current_version_url), headers=headers)
    formatted_response = json.loads(resp.text)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    #prYellow(formatted_response_str)

    try:
        measures_list = formatted_response.get('measures')
        query_fields=[]
        #print(measures_list)
        measures_counter = 0
        #prYellow("\r\n" + "Measures:")
        for x in measures_list:
            measures_counter += 1
            query_fields.append(x["field"])
        print("\r\n")
    except ValueError:
        prRed("there are no measures present in the dataset.")

    try:
        dimension_list = formatted_response.get('dimensions')
        dimension_counter = 0
        #prYellow("\r\n" + "Dimensions:")
        for x in dimension_list:
            field = x["field"]
            if field.endswith("_Second") or field.endswith("_Minute") or field.endswith("_Hour") or field.endswith("_Day") or field.endswith("_Week") or field.endswith("_Month") or field.endswith("_Quarter") or field.endswith("_Year") or field.endswith("_epoch"):
                pass
            else:
                dimension_counter += 1
                query_fields.append(x["field"])
                #print(field)
        #print(type(query_fields))
    except:
        prRed("there are no dimensions present in the dataset.")

    #def convert_list_to_string(query_fields, seperator=','):
    #    return seperator.join(query_fields)

    #query_fields_str = convert_list_to_string(query_fields)

    query_fields_str = ', '.join(f'\'{w}\'' for w in query_fields)

    total_fields = dimension_counter + measures_counter
    #print(total_fields)

    i = 1
    q_limit = 9999
    q_offset = 0
    dataset_extraction_dir = "dataset_extraction"

    #multithreaded function to submit the queries
    if batches_ > 0:
        threads = list()
        prCyan("\r\n" + "Starting {} CPU threads to extract the dataset".format(batches_) + "\r\n")
        _start = time.time()
        for index in range(batches_):
            x = threading.Thread(target=data_extract_thread, args=(dataset_,dataset_currentVersionId,query_fields_str,q_offset,q_limit,i,access_token,dataset_name,server_id,))
            threads.append(x)
            x.start()
            i += 1
            q_offset += 9999
            time.sleep(1)

        prCyan("\r\n" + "Progress: " + "\r\n")

        for index, thread in enumerate(threads):
            progress = ((index + 1) / batches_)*100
            progress = round(progress)
            if progress < 10:
                prYellow("  {}%".format(progress))
            elif progress < 30:
                prYellow(" {}%".format(progress))
            elif progress < 60:
                prLightPurple(" {}%".format(progress))
            elif progress < 100:
                prCyan(" {}%".format(progress))
            elif progress == 100:
                prGreen("{}%".format(progress))
            thread.join()
            time.sleep(0.5)

        _end = time.time()
        total_time = round((_end - _start),2)
        prGreen("\r\n" + "Multithreaded extraction completed in {}s.".format(total_time))
        time.sleep(1)

    if batches_ > 0:
        #Folder check for existing files - start:
        if os.path.exists("{}_dataset_extraction.csv".format(dataset_name)):
            os.remove("{}_dataset_extraction.csv".format(dataset_name))

        if os.path.exists('{}_{}_query_results.json'.format(dataset_name,i)):
            os.remove('{}_{}_query_results.json'.format(dataset_name,i))

        if os.path.exists('{}_{}_query_results.csv'.format(dataset_name,i)):
            os.remove('{}_{}_query_results.csv'.format(dataset_name,i))
        #Folder check for existing files - end.

        #Append all csv files from the batches - start:

        prGreen("\r\n" + "Compiling CSV.")
        extension = 'csv'
        _start = time.time()
        csv_files = glob.glob('{}_*.{}'.format(dataset_name,extension))
        combined_csv = pd.concat([pd.read_csv(csv_file,low_memory=False) for csv_file in csv_files])
        #print(combined_csv)
        combined_csv.to_csv( "{}_dataset_extraction.csv".format(dataset_name), index=False, encoding='utf-8-sig')
        _end = time.time()
        total_time = round((_end - _start),2)
        prGreen("\r\n" + "CSV compiled in {}s".format(total_time))
        time.sleep(0.2)
        prCyan("\r\n" + "Dataset Succesfully Exported." + "\r\n")
        time.sleep(0.5)
        prCyan("\r\n" + "Find the file here: {}".format(d_ext) + "\r\n")
        time.sleep(0.2)

        #Append all csv files from the batches - end.


    #Folder Cleanup
    i = 1
    while i <= batches_:
        #Remove partial json files
        os.remove('{}_{}_query_results.json'.format(dataset_name,i))
        #Remove partial csv files
        os.remove('{}_{}_query_results.csv'.format(dataset_name,i))
        i += 1

    #Go back to parent folder:
    os.chdir("..")
