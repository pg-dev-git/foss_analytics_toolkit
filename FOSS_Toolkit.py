import json, requests, os, configparser, datetime, time, multiprocessing as mp
from misc_tasks.terminal_colors import *
from misc_tasks.sfdc_login import *
from dataset_tasks.get_datasets  import *
from dataset_tasks.csv_new_dataset import *
from dataflow_tasks.get_dataflows import *
from data_manager_tasks.get_dataflowjobs import *
from art import *
from misc_tasks.initial_checks import *
from dashboards_tasks.get_dashboards import *
from dashboards_tasks.mass_dashboard_backup import *
from misc_tasks.get_ea_limits import *
from misc_tasks.line import *
from dataflow_tasks.mass_dataflows_backup import *
from dataset_tasks.mass_user_xmd_backup import *

if __name__ == "__main__":

    mp.freeze_support()

    d_ext = init_folders()

    os.chdir(d_ext)

    sfdc_login.intro()

    flag = "N"

    access_token,server_id,server_domain = sfdc_login.auth_check(flag)


    #config = configparser.ConfigParser()
    #config.read("{}".format(config_file))
    #server_id = config.get("DEFAULT", "server_id")

    run_token = True
    while run_token:
        line_print()
        prGreen("What do you want to do?:")
        time.sleep(0.15)
        prYellow("(Choose a number from the list below)" + "\r\n")
        time.sleep(0.2)
        prCyan("1 - List datasets")
        time.sleep(0.10)
        prCyan("2 - List dashboards")
        time.sleep(0.10)
        prCyan("3 - List dataflows")
        time.sleep(0.10)
        prCyan("4 - List Data Manager jobs")
        time.sleep(0.10)
        prCyan("5 - Create New Dataset from CSV")
        time.sleep(0.10)
        prLightPurple("6 - Mass Backup all Dataflows")
        time.sleep(0.10)
        prLightPurple("7 - Mass Backup all Dashboards")
        time.sleep(0.10)
        prLightPurple("8 - Mass Backup all User XMDs")
        time.sleep(0.10)
        prYellow("9 - Check TCRM Limits")
        time.sleep(0.10)
        prYellow("10 - Run Login Parameters Configuration")
        time.sleep(0.15)

        print("\r\n")
        user_input = input("Enter your selection: ")
        line_print()

        if user_input == "1":
            get_datasets(access_token,server_id,server_domain)

        if user_input == "2":
            get_dashboards_main(access_token,server_id,server_domain)

        if user_input == "3":
            get_dataflows(access_token,server_id,server_domain)

        if user_input == "4":
            get_dataflowsJobs(access_token,server_id,server_domain)

        if user_input == "5":
            new_csv_dataset(access_token,server_id,server_domain)

        if user_input == "6":
            mass_dataflows(access_token,server_id,server_domain)

        if user_input == "7":
            mass_dashboards(access_token,server_id,server_domain)

        if user_input == "8":
            mass_u_xmd_bkp(access_token,server_id,server_domain)

        if user_input == "9":
            get_EA_limits(access_token,server_id)

        if user_input == "10":
            flag = "Y"
            sfdc_login.auth_check(flag)
            flag = "N"


        check_token = "Y"

        if check_token == "Y":
            run_token = True
        elif check_token == "N":
            quit()
