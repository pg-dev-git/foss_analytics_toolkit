import json
import requests
from terminal_colors import *
import time

def get_dataflowsJobs_list(dataflowjobs_list):
    counter_2 = 0

    #Check running jobs
    counter = 0
    prCyan("\r\n" + "Checking for jobs currently running:")
    time.sleep(1)
    for x in dataflowjobs_list:

        if x["jobType"] != "datasync" and x["status"] == "Running":
            counter += 1
            counter_2 += 1
            print("{} - ".format(counter_2) ,"Job id:",x["id"],"- Status:",x["status"],"- Run Date:",x["executedDate"],"- Label:",x["label"])
    if counter == 0:
        prYellow("\r\n" + "There are no Dataflows running at the moment.")

    #Check failed jobs
    counter = 0
    prRed("\r\n" + "Checking for failed jobs:")
    time.sleep(1)
    for x in dataflowjobs_list:

        if x["jobType"] != "datasync" and x["status"] == "Failure":
            counter += 1
            counter_2 += 1
            print("{} - ".format(counter_2) ,"Job id:",x["id"],"- Status:",x["status"],"- Run Date:",x["executedDate"],"- Label:",x["label"])
    if counter == 0:
        prYellow("\r\n" + "There are no failed Dataflows.")

    #Check successfull jobs
    counter = 0
    prGreen("\r\n" + "Checking for succesfully completed jobs:")
    time.sleep(1)
    for x in dataflowjobs_list:

        if x["jobType"] != "datasync" and x["status"] == "Success":
            counter += 1
            counter_2 += 1
            print("{} - ".format(counter_2) ,"Job id:",x["id"],"- Status:",x["status"],"- Run Date:",x["executedDate"],"- Label:",x["label"])
    if counter == 0:
        prYellow("\r\n" + "There are no completed Dataflows.")

    #Check all other jobs
    counter = 0
    prYellow("\r\n" + "Checking all other status:")
    time.sleep(1)
    for x in dataflowjobs_list:

        if x["jobType"] != "datasync" and (x["status"] != "Success" and x["status"] != "Failure" and x["status"] != "Running"):
            counter += 1
            counter_2 += 1
            print("{} - ".format(counter_2) ,"Job id:",x["id"],"- Status:",x["status"],"- Run Date:",x["executedDate"],"- Label:",x["label"])
    if counter == 0:
        prYellow("\r\n" + "There are no available Dataflows.")

    print("\r\n")

def get_datasyncJobs_list(dataflowjobs_list):
    counter_2 = 0

    #Check running jobs
    counter = 0
    prCyan("\r\n" + "Checking for jobs currently running:")
    time.sleep(1)
    for x in dataflowjobs_list:

        if x["jobType"] == "datasync" and x["status"] == "Running":
            counter += 1
            counter_2 += 1
            print("{} - ".format(counter_2) ,"Job id:",x["id"],"- Status:",x["status"],"- Run Date:",x["executedDate"],"- Label:",x["label"])
    if counter == 0:
        prYellow("\r\n" + "There are no Datasyncs running at the moment.")

    #Check failed jobs
    counter = 0
    prRed("\r\n" + "Checking for failed jobs:")
    time.sleep(1)
    for x in dataflowjobs_list:

        if x["jobType"] == "datasync" and x["status"] == "Failure":
            counter += 1
            counter_2 += 1
            print("{} - ".format(counter_2) ,"Job id:",x["id"],"- Status:",x["status"],"- Run Date:",x["executedDate"],"- Label:",x["label"])
    if counter == 0:
        prYellow("\r\n" + "There are no failed Datasyncs.")

    #Check successfull jobs
    counter = 0
    prGreen("\r\n" + "Checking for succesfully completed jobs:")
    time.sleep(1)
    for x in dataflowjobs_list:

        if x["jobType"] == "datasync" and x["status"] == "Success":
            counter += 1
            counter_2 += 1
            print("{} - ".format(counter_2) ,"Job id:",x["id"],"- Status:",x["status"],"- Run Date:",x["executedDate"],"- Label:",x["label"])
    if counter == 0:
        prYellow("\r\n" + "There are no completed Datasyncs.")

    #Check all other jobs
    counter = 0
    prYellow("\r\n" + "Checking all other status:")
    time.sleep(1)
    for x in dataflowjobs_list:

        if x["jobType"] == "datasync" and (x["status"] != "Success" and x["status"] != "Failure" and x["status"] != "Running"):
            counter += 1
            counter_2 += 1
            print("{} - ".format(counter_2) ,"Job id:",x["id"],"- Status:",x["status"],"- Run Date:",x["executedDate"],"- Label:",x["label"])
    if counter == 0:
        prYellow("\r\n" + "There are no other available Datasyncs.")

def get_AllJobs_list(dataflowjobs_list):
    counter_2 = 0

    #Check running jobs
    prGreen("\r\n" + "Checking for job types:")
    counter = 0
    time.sleep(1)
    for x in dataflowjobs_list:
        counter += 1
        counter_2 += 1
        print("{} - ".format(counter_2) ,"Job id:",x["id"],"- Status:",x["status"],"- Run Date:",x["executedDate"],"- Label:",x["label"])
    if counter == 0:
        prYellow("\r\n" + "There are no jobs available.")
