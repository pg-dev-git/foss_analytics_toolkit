import json
import requests
from terminal_colors import *
from sfdc_login import *
from dataset_tasks.get_datasets import *
from dataset_tasks.new_csv_dataset import *
#from get_dashboards import *
from dataflow_tasks.get_dataflows import *
from data_manager_tasks.get_dataflowjobs import *
from art import *
import os
from initial_checks import *
import configparser
import datetime
import time
from dashboards_tasks.get_dashboards import *

if __name__ == "__main__":

    d_ext = init_folders()

    os.chdir(d_ext)

    sfdc_login.intro()




    #Beta Lock - Start
    current_time = datetime.datetime.now()

    try:
        if (current_time.year) == 2021 and (current_time.month) == 4 and (current_time.day) <= 30:
            print("\r\n" + "Welcome to the beta testing. Please try all the features and share your feedback!" + "\r\n")
            time.sleep(1)
        else:
            print("\r\n" + "The beta test period has expired." + "\r\n")
            quit()
    except ValueError:
        print("\r\n" + "The beta test period has expired." + "\r\n")
        quit()
    #Beta Lock - End

    flag = "N"

    config_file,access_token = sfdc_login.auth_check(flag)


    config = configparser.ConfigParser()
    config.read("{}".format(config_file))
    server_id = config.get("DEFAULT", "server_id")



    run_token = True
    while run_token:
        prGreen("What do you want to do?:")
        time.sleep(0.3)
        prYellow("(Choose a number from the list below)" + "\r\n")
        time.sleep(0.5)
        prCyan("1 - List datasets")
        time.sleep(0.15)
        prCyan("2 - List dashboards")
        time.sleep(0.15)
        prCyan("3 - List dataflows")
        time.sleep(0.15)
        prCyan("4 - List Data Manager jobs")
        time.sleep(0.15)
        prCyan("5 - Create New Dataset from CSV")
        time.sleep(0.15)
        prYellow("6 - Run Login Parameters Configuration")
        time.sleep(0.5)

        #prCyan("5 - Upload a CSV Dataset - New/Override")
        print("\r\n")
        user_input = input("Enter your selection: ")

        if user_input == "1":
            get_datasets(access_token,server_id)

        if user_input == "2":
            get_dashboards(access_token,server_id)

        if user_input == "3":
            get_dataflows(access_token,server_id)

        if user_input == "4":
            get_dataflowsJobs(access_token,server_id)

        if user_input == "5":
            upload_new_csv_dataset(access_token,server_id)

        if user_input == "6":
            flag = "Y"
            sfdc_login.auth_check(flag)
            flag = "N"

        print("\r\n")

        #check_token = input("Do you want to do something else (Y) or exit the program (N)?" + "\r\n")

        check_token = "Y"

        if check_token == "Y":
            run_token = True
        elif check_token == "N":
            quit()
