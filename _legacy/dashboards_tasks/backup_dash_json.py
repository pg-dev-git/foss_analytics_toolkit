import json, requests, time, sys, subprocess
from misc_tasks.terminal_colors import *
from misc_tasks.sfdc_login import *
from dashboards_tasks.get_dash_datasets import *
from misc_tasks.zipper import *

def remove(string):
    return string.replace(" ", "_")

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

def backup_dash_json(access_token,dashboard_,server_id,dashboard_label,server_domain):

    try:
        dataflow_extraction_dir = "dashboard_backup"
        os.mkdir(dataflow_extraction_dir)
    except OSError as error:
            pass

    cd = os.getcwd()

    os_ = sfdc_login.get_platform()

    if os_ == "Windows":
        d_ext = "{}".format(cd)+"\\dashboard_backup\\"
    else:
        d_ext = "{}".format(cd)+"/dashboard_backup/"

    os.chdir(d_ext)

    prGreen("\r\nRetrieving JSON Definition...")
    time.sleep(1)
    headers = {
        'Authorization': "Bearer {}".format(access_token)
        }
    resp = requests.get('https://{}.my.salesforce.com/services/data/v53.0/wave/dashboards/{}'.format(server_domain,dashboard_), headers=headers)

    formatted_response = json.loads(resp.text)
    formatted_response_str = json.dumps(formatted_response, indent=2)

    state_ = formatted_response.get("state")
    label_ = formatted_response.get("label")
    mobileDisabled_ = formatted_response.get("mobileDisabled")
    datasets_ = formatted_response.get("datasets")

    json_backup = {}

    json_backup['label'] = label_
    json_backup['mobileDisabled'] = mobileDisabled_
    json_backup['state'] = state_
    json_backup['datasets'] = datasets_

    string = dashboard_label

    try:
        dash_name = remove(string)
    except:
        dash_name = dashboard_label

    with open('{}_{}_backup.json'.format(dash_name,dashboard_), 'w') as outfile:
        json.dump(json_backup, outfile)

    os_running = get_platform()

    if os_running == "Windows":

        a_ = "(Get-Content " + '{}_{}_backup.json'.format(dash_name,dashboard_) + ").Replace('&quot;','\\\"') | Set-Content " + '{}_{}_backup.json'.format(dash_name,dashboard_)
        b_ = "(Get-Content " + '{}_{}_backup.json'.format(dash_name,dashboard_) + ").Replace('&#39;','''') | Set-Content " + '{}_{}_backup.json'.format(dash_name,dashboard_)
        c_ = "(Get-Content " + '{}_{}_backup.json'.format(dash_name,dashboard_) + ").Replace('&gt;','>') | Set-Content " + '{}_{}_backup.json'.format(dash_name,dashboard_)
        d_ = "(Get-Content " + '{}_{}_backup.json'.format(dash_name,dashboard_) + ").Replace('&lt;','<') | Set-Content " + '{}_{}_backup.json'.format(dash_name,dashboard_)
        e_ = "(Get-Content " + '{}_{}_backup.json'.format(dash_name,dashboard_) + ").Replace('&amp;','&') | Set-Content " + '{}_{}_backup.json'.format(dash_name,dashboard_)
        ps_1_dict = {"a": "{}".format(a_), "b": "{}".format(b_), "c": "{}".format(c_), "d": "{}".format(d_), "e": "{}".format(e_)}

        for x in ps_1_dict.values():
            completed = subprocess.run(["powershell", "-Command", x], capture_output=True)
            time.sleep(0.5)

        prCyan("\r\nDashboard JSON definition succesfully backed up here: ")
        prLightPurple("\r\n{}".format(d_ext))
        line_print()
        time.sleep(1)

        os.chdir("..")

    elif os_running == 'Linux' or os_running == 'OS X' or os_running == 'linux':
        a_ = "sed -i 's/&quot;/\\\"/g'" + '{}/{}_{}_backup.json'.format(d_ext,dash_name,dashboard_)
        b_ = "'s/&#39;/''''/g'" + '{}_{}_backup.json'.format(dash_name,dashboard_)
        c_ = "'s/&gt;/'>'/g'" + '{}_{}_backup.json'.format(dash_name,dashboard_)
        d_ = "'s/&lt;/'<'/g'" + '{}_{}_backup.json'.format(dash_name,dashboard_)
        e_ = "'s/&amp;/'&'/g'" + '{}_{}_backup.json'.format(dash_name,dashboard_)
        ps_1_dict = {"a": "{}".format(a_), "b": "{}".format(b_), "c": "{}".format(c_), "d": "{}".format(d_), "e": "{}".format(e_)}

        print(subprocess.call('sed -i \'s/\&quot;/\\\\"/g\' {}{}_{}_backup.json'.format(d_ext,dash_name,dashboard_), shell=True))
        print(subprocess.call('sed -i "s/\&#39;/\'/g" {}{}_{}_backup.json'.format(d_ext,dash_name,dashboard_), shell=True))
        print(subprocess.call('sed -i "s/\&gt;/>/g" {}{}_{}_backup.json'.format(d_ext,dash_name,dashboard_), shell=True))
        print(subprocess.call('sed -i "s/\&lt;/</g" {}{}_{}_backup.json'.format(d_ext,dash_name,dashboard_), shell=True))

        prCyan("\r\nDashboard JSON definition succesfully backed up here: ")
        prLightPurple("\r\n{}".format(d_ext))
        line_print()
        time.sleep(1)

        os.chdir("..")

    else:
        prRed("\r\nThere is no JSON available to backup.\r\n")
        os.chdir("..")
        line_print()
        time.sleep(1)
