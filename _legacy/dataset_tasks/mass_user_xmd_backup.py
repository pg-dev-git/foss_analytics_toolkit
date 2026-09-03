import json, requests, time, queue, threading, datetime, shutil
from misc_tasks.terminal_colors import *
from misc_tasks.sfdc_login import *
#from dataset_tasks.get_dataset_field_detail import *
#from dataset_tasks.get_dataset_extract_v2 import *
#from dataset_tasks.get_dataset_extract_MP import *
#from dataset_tasks.upload_dataset import *
#from dataset_tasks.dataset_backup_user_xmd import *
#from dataset_tasks.xmd_cleanup import *
#from dataset_tasks.append_dataset import *
#from dataset_tasks.delete_dataset import *
#from dataset_tasks.get_dataset_history import *
#from dataset_tasks.get_dataset_dependencies import *
from misc_tasks.line import *
from misc_tasks.zipper import *


def mass_u_xmd_bkp(access_token,server_id,server_domain):

    now = datetime.datetime.now()

    dt_string = now.strftime("%d-%m-%Y_%H_%M")

    #Backup folder creation - start:
    try:
        dataset_xmd_dir = "xmd_backups"
        os.mkdir(dataset_xmd_dir)
    except OSError as error:
            print(" ")
    #Backup folder creation - end.

    cd = os.getcwd()
    #print(cd)

    os_ = sfdc_login.get_platform()

    if os_ == "Windows":
        d_ext = "{}".format(cd)+"\\xmd_backups\\"
    else:
        d_ext = "{}".format(cd)+"/xmd_backups/"

    #print(d_ext)

    os.chdir(d_ext)

    try:
        xmd_extraction_dir = 'mass_xmd_backup_{}'.format(dt_string)
        os.mkdir(xmd_extraction_dir)
    except OSError as error:
            pass

    cd = os.getcwd()

    d_ext = "{}".format(cd)+"\\{}\\".format(xmd_extraction_dir)

    os.chdir(d_ext)


    prGreen("\r\n" + "Checking Datasets..." + "\r\n")
    line_print()
    _start = time.time()
    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }
    resp = requests.get('https://{}.my.salesforce.com/services/data/v53.0/wave/datasets'.format(server_domain), headers=headers)
    #print(resp.json())
    #Print PrettyJSON in Terminal

    formatted_response = json.loads(resp.text)
    #print(formatted_response)
    formatted_response_str = json.dumps(formatted_response, indent=2)
    #prGreen(formatted_response_str)

    datasets_list = formatted_response.get('datasets')

    counter = 0
    counterx = 0

    for xx in datasets_list:
        counterx += 1

    i = 0

    que = queue.Queue()
    threads = list()

    t_result = []

    for index in range(counterx):
        try:
            cvl = datasets_list[index]["currentVersionUrl"]
            currentName = datasets_list[index]['name']
            params = [server_id,access_token,cvl,i,currentName,server_domain]
            x = threading.Thread(target=lambda q, arg1: q.put(xmd_bkp_mt(arg1)), args=(que,params))
            threads.append(x)
            x.start()
            time.sleep(0.15)
        except:
            print("\r\nNo XMD available for {}".format(currentName))


    for index, thread in enumerate(threads):
        thread.join()
        time.sleep(0.1)

    while not que.empty():
        t_result.append(que.get())

    _end = time.time()
    total_time = round((_end - _start),2)
    os.chdir("..")
    directory = './{}'.format(xmd_extraction_dir)
    line_print()
    tcrm_zipper(directory,xmd_extraction_dir)
    shutil.rmtree(r'./{}'.format(xmd_extraction_dir))
    prGreen("\r\n" + "Mass Backup succesfully completed in {}s.".format(total_time))
    line_print()
    cd = os.getcwd()
    prCyan("\r\n" + "Find the files here: ")
    prLightPurple("\r\n" + "{}".format(cd))
    os.chdir("..")


def xmd_bkp_mt(params):

    server_id = params[0]
    access_token = params[1]
    cvl = params[2]
    i = params[3]
    currentName = params[4]
    server_domain = params[5]


    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }
    xmds_url = "https://{}.my.salesforce.com".format(server_domain) + "{}".format(cvl) + "/xmds/user"
    xmds_json = requests.get('{}'.format(xmds_url), headers=headers)
    xmds_json = json.loads(xmds_json.text)

    xmds_json.pop('createdBy')
    xmds_json.pop('url')
    xmds_json.pop('type')
    xmds_json.pop('lastModifiedDate')
    xmds_json.pop('lastModifiedBy')
    xmds_json.pop('language')
    xmds_json.pop('createdDate')
    xmds_json.pop('dataset')

#Cleanup Derived Measures - Start:
    try:
        deriv_meas_format = xmds_json.get('derivedMeasures')
        for x in deriv_meas_format:
            #fields_counter += 1
            if x['format']:
                format_ = x['format']
                #format_ = format.get('customFormat')
                format_ = format_['customFormat']
                new_form = format_.replace('&quot;','\"')
                x['format']['customFormat'] = new_form
        xmds_json.pop('derivedMeasures')
        xmds_json['derivedMeasures'] = deriv_meas_format
    except:
        pass
#Cleanup Derived Measures - End.

    with open('{}_backup_user.xmd.json'.format(currentName), 'w') as outfile:
        json.dump(xmds_json, outfile)

    #print("\r\n" + "Done backing up:")
    prLightPurple("{} ".format(currentName))

    return i
