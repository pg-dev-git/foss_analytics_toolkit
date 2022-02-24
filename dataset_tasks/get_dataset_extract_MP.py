import json, requests, math, csv, glob, os, base64, time, sys, subprocess, gc, psutil
from terminal_colors import *
from sfdc_login import *
import modin.pandas as pd
import multiprocessing as mp
from dataset_tasks.dataset_extract_MP import *
from line import *
from dataset_tasks.MP_control import *
from dataset_tasks.mt_for_mp import *
from b2h import *
from system_metrics import *

class Result():
    def __init__(self):
        self.val = 0

    def update_result(self, val):
        self.val += 1

def get_platform():
    platforms = {
        'linux1' : 'Linux',
        'linux2' : 'Linux',
        'darwin' : 'OS X',
        'win32' : 'Windows'
    }
    if sys.platform not in platforms:
        return sys.platform

    return platforms[sys.platform]

def delete_last():
    sys.stdout.write('\x1b[1A')
    sys.stdout.write('\x1b[2K')

def get_datasets_extract_mp(access_token,dataset_,server_id,dataset_rows,server_domain):

    try:
        dataset_extraction_dir = "dataset_extraction"
        os.mkdir(dataset_extraction_dir)
    except OSError as error:
            pass

    cd = os.getcwd()
    #print(cd)

    os_ = get_platform()

    if os_ == "Windows":
        d_ext = "{}".format(cd)+"\\dataset_extraction\\"
    else:
        d_ext = "{}".format(cd)+"/dataset_extraction/"

    #print(d_ext)

    os.chdir(d_ext)

    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }
    resp = requests.get('https://{}.my.salesforce.com/services/data/v53.0/wave/datasets/{}'.format(server_domain,dataset_), headers=headers)

    formatted_response = json.loads(resp.text)
    #print(formatted_response)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    #prGreen(formatted_response_str)


    dataset_current_version_url = formatted_response.get('currentVersionUrl')
    dataset_currentVersionId = formatted_response.get('currentVersionId')
    dataset_name = formatted_response.get('name')

    try:
        dataset_extraction_dir = dataset_name
        os.mkdir(dataset_extraction_dir)
    except OSError as error:
            pass

    cd = os.getcwd()
    #print(cd)

    if os_ == "Windows":
        d_ext = "{}".format(cd)+"\\{}\\".format(dataset_name)
    else:
        d_ext = "{}".format(cd)+"/{}/".format(dataset_name)

    os.chdir(d_ext)


    saql = "q = load \"{}/{}\";q = group  q by all;q = foreach q generate count() as 'count';q = limit q 1;".format(dataset_,dataset_currentVersionId)

    saql_payload = {"name": "get_rows","query": str(saql), "queryLanguage": "SAQL"}

    saql_payload = json.dumps(saql_payload)

    headers = {'Authorization': "Bearer {}".format(access_token),
               'Content-Type': "application/json"
               }

    resp = requests.post('https://{}.my.salesforce.com/services/data/v53.0/wave/query'.format(server_domain), headers=headers, data=saql_payload)
    query_results = json.loads(resp.text)
    count_rows = query_results.get('results')
    count_rows = count_rows['records']
    count_rows = count_rows[0]
    count_rows = count_rows.get('count')
    batches_ = math.ceil(count_rows / 50000)
    #print(batches_)

    try:
        #Folder Cleanup
        i = 0
        while i <= batches_:
            #Remove partial json files
            if os.path.exists('{}_{}_query_results.csv'.format(dataset_name,i)):
                os.remove('{}_{}_query_results.json'.format(dataset_name,i))
            #Remove partial csv files
            if os.path.exists('{}_{}_query_results.csv'.format(dataset_name,i)):
                os.remove('{}_{}_query_results.csv'.format(dataset_name,i))
            i += 1
    except:
        pass

    dataset_current_version_url = "https://{}.my.salesforce.com".format(server_domain) + "{}".format(dataset_current_version_url) + "/xmds/main"

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
            field = x["field"]
            if field.endswith("_epoch"):
                pass
            else:
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

    except ValueError:
        prRed("there are no dimensions present in the dataset.")

    #def convert_list_to_string(query_fields, seperator=','):
    #    return seperator.join(query_fields)

    #query_fields_str = convert_list_to_string(query_fields)


    query_fields_str = ', '.join(f'\'{w}\'' for w in query_fields)

    total_fields = dimension_counter + measures_counter
    #print(total_fields)

    i = 0
    q_limit = 50000
    q_offset = 0
    dataset_extraction_dir = "dataset_extraction"

    #print("start")
    #multicore & threads function to submit the queries
    if batches_ > 0:


        ram_av = round((( psutil.virtual_memory()[0] /  1024 ) / 1024) / 1024)

        ram_req = round((0.00000029296875 * dataset_rows),2) + (round((0.00000029296875 * dataset_rows),2) * 0.1)

        ram_rem = round((( psutil.virtual_memory()[1] /  1024 ) / 1024) / 1024)

        if ram_rem <= ram_req:
            print("Warning:",end='')
            prRed("There is not enough RAM available in the system to export this dataset as a single file.")
            line_print()

            prGreen("\r\n" + "What do you want to do?:")
            time.sleep(0.2)
            prYellow("(Choose a number from the list below)" + "\r\n")
            time.sleep(0.3)
            prCyan("1 - Export the dataset in multiple files.")
            time.sleep(0.15)
            prCyan("2 - Cancel the operation.")
            time.sleep(0.15)

            user_input = input("\r\nEnter your selection: ")
            line_print()

            ok_flag = 0

            while ok_flag != 1:

                if user_input == "1":
                    multi_file_flag = True
                    ok_flag = 1

                if user_input == "2":
                    os.chdir("..")
                    prYellow("Cancelling the extraction process...")
                    time.sleep(2)
                    ok_flag = 1
                    quit()

                if user_input != '1' and user_input != '2':
                    print('Error:',end='')
                    prRed('Enter a valid option. 1 or 2.')
                    user_input = input("\r\nEnter your selection: ")

                    if user_input == "1":
                        multi_file_flag = True
                        ok_flag = 1

                    if user_input == "2":
                        os.chdir("..")
                        prYellow("Cancelling the extraction process...")
                        time.sleep(2)
                        ok_flag = 1
                        quit()

                    line_print()

        else:
            prGreen("How do you want to export the dataset?:")
            time.sleep(0.2)
            prYellow("(Choose a number from the list below)\r\n")
            time.sleep(0.3)
            prCyan("1 - As a single CSV file.")
            time.sleep(0.15)
            prCyan("2 - As multiple CSV files.")
            time.sleep(0.15)

            user_input = input("\r\nEnter your selection: ")
            line_print()

            ok_flag = 0

            while ok_flag != 1:

                if user_input == "1":
                    multi_file_flag = False
                    ok_flag = 1

                if user_input == "2":
                    multi_file_flag = True
                    ok_flag = 1

                if user_input != '1' and user_input != '2':
                    print('Error:',end='')
                    prRed('Enter a valid option. 1 or 2.')
                    user_input = input("\r\nEnter your selection: ")

                    if user_input == "1":
                        multi_file_flag = False
                        ok_flag = 1

                    if user_input == "2":
                        multi_file_flag = True
                        ok_flag = 1

                    line_print()

        pool = mp.Pool((mp.cpu_count()))
        cpus = int(mp.cpu_count())
        mts = math.ceil(batches_ / cpus)
        pool_cycles_A = math.ceil(batches_ / cpus)
        pool_cycles_B = math.floor(batches_ / cpus)
        control_flag = mts
        control = round(batches_ / 2)
        thread_count = 0
        thread_id = 0
        yy = 1
        remain_jobs = batches_
        cpu_control = 0
        cpus_required = 0
        result = Result()
        prCyan("\r\n" + "Starting extraction using all {} CPU Cores...".format(cpus))
        line_print()
        prCyan("\r\n\r\n\r\n")


        while cpu_control < batches_:
            cpu_control += pool_cycles_A + 1
            cpus_required += 1

        #print(cpus,cpus_required)
        #prCyan("\r\n" + "\r\n" + "\r\n" + "\r\n")

        smax_req = math.ceil(90 / cpus_required)

        try:
            del_ = 0
            for del_ in range(cpus):
                if os.path.exists('mp{}.ini'.format(del_)):
                    os.remove('mp{}.ini'.format(del_))
                if os.path.exists('p{}.ini'.format(del_)):
                    os.remove('p{}.ini'.format(del_))
                    del_ += 1
        except:
            pass

        try:
            if cpus < 10 and ram_av >= 16:
                max_t_count = int(cpus_required)
            elif cpus < 10 and ram_av < 16:
                max_t_count = int(cpus_required - 2)
            elif cpus > 9 and ram_av >= 16:
                max_t_count = int(max_req)
            elif cpus > 9 and ram_av < 16:
                if ( max_req - 1 ) == 0:
                    max_t_count = 1
                else:
                    max_t_count = int(( max_req - 2 ))
            else:
                max_t_count = 4
        except:
            max_t_count = 4

        for zz in range(cpus_required):
            #print("zz {}".format(zz))
            #print("cpus_required {}".format(cpus_required))
            if thread_count < batches_ and (zz + 1) < cpus_required:
                batches_mt = pool_cycles_A + 1
                thread_count += batches_mt
            else:
                batches_mt = batches_ - thread_count

            control_files(access_token,dataset_,server_id,zz,batches_mt,thread_id,cpus_required,cpus)

            thread_id += batches_mt

            remain_jobs = batches_ - batches_mt

            sum_control = batches_mt + thread_count

        _start = time.time()
        down_start = time.time()

        #prRed("Calling MP Function now.")
        #result_async = [pool.apply_async(data_extract_mp, args = (dataset_,dataset_currentVersionId,query_fields_str,q_offset,q_limit,i,access_token,dataset_name,server_id,batches_,query_fields, )) for i in
                        #range(batches_)]

        result_async = [pool.apply_async(mp_to_mt, args = (access_token,dataset_,server_id,dataset_name,dataset_currentVersionId,query_fields_str,q_limit,i,max_t_count,cpus_required,server_domain, ), callback=result.update_result) for i in
                        range(cpus_required)]

        #results_proc = []

        if batches_ >= cpus_required:
            if cpus_required < cpus:
                s_time = 0.75
            else:
                s_time = 1
            try:
                yyy = 0
                xxx = 0
                zzz = 0
                www = 0
                progress = 0
                prog_ini_top = 0
                for r in result_async:
                    #results_proc.append(r.get())
                    #print(results_proc)
                    while yyy < batches_:
                        xxx = 0
                        zzz = 0
                        www = 1
                        for xxx in range(cpus_required):

                            #print("xxx for {}".format(xxx))
                            #time.sleep(2)
                            try:
                                if batches_ > 10:
                                    if os.path.exists("p{}.ini".format(xxx)):
                                        #print(xxx)
                                        #print("p{}.ini".format(xxx))
                                        config = configparser.ConfigParser()
                                        config.read("p{}.ini".format(xxx))
                                        #print(config.get("DEFAULT", "progress"))
                                        prog_ini = float(config.get("DEFAULT", "progress")) * cpus_required
                                        #print(prog_ini)


                                        if (prog_ini > prog_ini_top):
                                            prog_ini_top = prog_ini
                                            yyy += prog_ini_top
                                        elif prog_ini <= prog_ini_top:
                                            www += 1

                                        if www == cpus_required:
                                            yyy += ( prog_ini_top ) * 2


                                        progress = round((yyy / ( batches_ ) ) * 100,1)
                                else:
                                    progress += ( result.val ) * 10
                                    yyy = progress

                                    #zzz += 1

                                #print("yyy {}".format(yyy) + " progress {}".format(progress) + " www {}".format(www) + " cpus_required {}".format(cpus_required) + " batches: {}".format(batches_) + "\r\n" + "\r\n"+ "\r\n")
                                if progress < 10:
                                    delete_last()
                                    delete_last()
                                    delete_last()
                                    delete_last()
                                    get_sys_metrics_dn()
                                    print("Download Progress:", end='')
                                    prYellow("{}%\r".format(progress))
                                elif progress < 30:
                                    delete_last()
                                    delete_last()
                                    delete_last()
                                    delete_last()
                                    get_sys_metrics_dn()
                                    print("Download Progress:", end='')
                                    prYellow("{}%\r".format(progress))
                                elif progress < 60:
                                    delete_last()
                                    delete_last()
                                    delete_last()
                                    delete_last()
                                    get_sys_metrics_dn()
                                    print("Download Progress:", end='')
                                    prLightPurple("{}%\r".format(progress))
                                elif progress < 100:
                                    delete_last()
                                    delete_last()
                                    delete_last()
                                    delete_last()
                                    get_sys_metrics_dn()
                                    print("Download Progress:", end='')
                                    prCyan("{}%\r".format(progress))
                                elif progress > 100:
                                    delete_last()
                                    delete_last()
                                    delete_last()
                                    delete_last()
                                    get_sys_metrics_dn()
                                    print("Download Progress:", end='')
                                    prCyan(" 99%\r")

                                    #time.sleep(0.25)
                                #print("xxx {}".format(xxx))
                                #print("yyy {}".format(yyy))
                                #print("batches_ {}".format(batches_))
                                #print("\r\n")
                            except:
                                #xxx -= 1
                                pass
            except:
                pass

        #results = [r.get() for r in result_async]

        pool.close()
        pool.join()
        delete_last()
        print("Download Progress:", end='')
        prGreen("100%\r")
        down_end = time.time()
        total_down = round((down_end - down_start),2)
        prCyan("\r\nData downloaded in {}s".format(total_down))
        line_print()

    del_ = 0
    for del_ in range(cpus_required):
        if os.path.exists('mp{}.ini'.format(del_)):
            os.remove('mp{}.ini'.format(del_))
        if os.path.exists('p{}.ini'.format(del_)):
            os.remove('p{}.ini'.format(del_))
            del_ += 1

    if batches_ > 0:
        #Folder check for existing files - start:
        if os.path.exists("{}_dataset_extraction.csv".format(dataset_name)):
            os.remove("{}_dataset_extraction.csv".format(dataset_name))

        try:
            del_ = 0
            for del_ in range(batches_):
                if os.path.exists('{}_{}_query_results.csv'.format(dataset_name,del_)):
                    os.remove('{}_{}_query_results.csv'.format(dataset_name,del_))
                del_ += 1
        except:
            pass
        #Folder check for existing files - end.

        #Append all csv files from the batches - start:

        if multi_file_flag == False:
            prGreen("\r\nCompiling CSV:")
            extension = 'csv'
            csv_files = glob.glob('{}_*split*.{}'.format(dataset_name,extension))
            #print('RAM memory total:', (((psutil.virtual_memory()[0] / 1024) / 1024 )/1024))
            #print('RAM memory available:', (((psutil.virtual_memory()[1] / 1024) / 1024 )/1024))
            #print('RAM memory % used:', psutil.virtual_memory()[2])
            #print('RAM memory total used:', (((psutil.virtual_memory()[3] / 1024) / 1024 )/1024))
            #print('RAM memory total free:', (((psutil.virtual_memory()[4] / 1024) / 1024 )/1024))
            #print(csv_files)
            #quit()
            csv_con_start = time.time()
            combined_csv = pd.concat([pd.read_csv(csv_file, low_memory=False) for csv_file in csv_files])
            csv_con_end = time.time()
            total_csv = round((csv_con_end - csv_con_start),2)
            prCyan("\r\nCSVs Appended in {}s".format(total_csv))
            #print('RAM memory total:', (((psutil.virtual_memory()[0] / 1024) / 1024 )/1024))
            #print('RAM memory available:', (((psutil.virtual_memory()[1] / 1024) / 1024 )/1024))
            #print('RAM memory % used:', psutil.virtual_memory()[2])
            #print('RAM memory total used:', (((psutil.virtual_memory()[3] / 1024) / 1024 )/1024))
            #print('RAM memory total free:', (((psutil.virtual_memory()[4] / 1024) / 1024 )/1024))
            csv_start = time.time()
            combined_csv.fillna(0)
            combined_csv.to_csv( "{}_dataset_extraction.csv".format(dataset_name), index=False, header=True, encoding='utf-8')

            #cmd = "Get-ChildItem -Filter *_results.csv | Select-Object -ExpandProperty FullName | Import-Csv | Export-Csv .\{}_extract.csv -NoTypeInformation".format(dataset_name)
            #completed = subprocess.run(["powershell", "-Command", cmd], capture_output=True)

            csv_end = time.time()
            total_csv = round((csv_end - csv_start),2)

            prCyan("\r\nFinal CSV Built in {}s".format(total_csv))
            #print('RAM memory total:', (((psutil.virtual_memory()[0] / 1024) / 1024 )/1024))
            #print('RAM memory available:', (((psutil.virtual_memory()[1] / 1024) / 1024 )/1024))
            #print('RAM memory % used:', psutil.virtual_memory()[2])
            #print('RAM memory total used:', (((psutil.virtual_memory()[3] / 1024) / 1024 )/1024))
            #print('RAM memory total free:', (((psutil.virtual_memory()[4] / 1024) / 1024 )/1024))
            line_print()
            prGreen("\r\nData sample:\r\n")
            print(combined_csv)
            line_print()
            prCyan("\r\nFind the file here: {}\r\n".format(d_ext))
            time.sleep(0.2)
            del combined_csv
            gc.collect()

            #Folder Cleanup
            del_ = 0
            for del_ in range(batches_):
                if os.path.exists('{}_dataset_extraction_split{}.csv'.format(dataset_name,del_)):
                    os.remove('{}_dataset_extraction_split{}.csv'.format(dataset_name,del_))
                    del_ += 1

        elif multi_file_flag:
            prCyan("\r\nFind the files here: {}\r\n".format(d_ext))
            #prGreen("\r\n" + "Compiling CSV:" + "\r\n")
            #extension = 'csv'
            #csv_files = glob.glob('{}_*split*.{}'.format(dataset_name,extension))
            #csv_con_start = time.time()
            #combined_csv = pd.concat([pd.read_csv(csv_file, low_memory=False) for csv_file in csv_files])
            #csv_con_end = time.time()
            #total_csv = round((csv_con_end - csv_con_start),2)
            #prCyan("\r\n" + "CSVs Appended in {}s".format(total_csv) + "\r\n")
            #csv_start = time.time()
            #combined_csv.fillna(0)
            #rem_rows_start = 0
            #rem_rows_end = 0
            #file_id = 1
            #while rem_rows > 0:
            #    rem_rows_end += 1000000
            #    combined_csv.iloc[rem_rows_start:rem_rows_end].to_csv("{}_extraction_split_{}.csv".format(dataset_name,file_id), index=False, header=True, encoding='utf-8')
            #    rem_rows_start += 1000000
            #    rem_rows = dataset_rows - 1000000
            #    file_id += 1

        #Append all csv files from the batches - end.

    _end = time.time()
    total_time = round((_end - _start),2)
    prGreen("\r\nExtraction completed in {}s.".format(total_time))
    time.sleep(0.2)
    line_print()

    #Go back to parent folder:
    os.chdir("..")
    os.chdir("..")
