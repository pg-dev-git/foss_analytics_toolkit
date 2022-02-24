import json
import requests
from terminal_colors import *
import time
from line import *
from data_manager_tasks.get_dataflowjob_id import *

def get_dataflowsJobs_list(dataflowjobs_list):
    counter_2 = 0

    #Check running jobs
    counter = 0
    prCyan("\r\n" + "Checking for jobs currently running:")
    time.sleep(2)
    for x in dataflowjobs_list:

        if x["jobType"] != "datasync" and x["status"] == "Running":
            counter += 1
            counter_2 += 1
            ex_date = x["executedDate"]
            ex_date = ex_date[:10]
            ex_time = x["executedDate"]
            ex_time = ex_time[11:19]
            print("{} - ".format(counter_2) ,"Job id:",x["id"],"- Status:",x["status"],"- Run Date:","{} {}".format(ex_date,ex_time),"- Label:",x["label"])

    if counter == 0:
        prYellow("\r\n" + "There are no Dataflows running at the moment.")

    line_print()

    #Check failed jobs
    counter = 0
    prRed("\r\n" + "Checking for failed jobs:")
    time.sleep(2)
    for x in dataflowjobs_list:

        if x["jobType"] != "datasync" and x["status"] == "Failure":
            counter += 1
            counter_2 += 1
            ex_date = x["executedDate"]
            ex_date = ex_date[:10]
            ex_time = x["executedDate"]
            ex_time = ex_time[11:19]
            print("{} - ".format(counter_2) ,"Job id:",x["id"],"- Status:",x["status"],"- Run Date:","{} {}".format(ex_date,ex_time),"- Label:",x["label"])

    if counter == 0:
        prYellow("\r\n" + "There are no failed Dataflows.")

    line_print()

    #Check successfull jobs
    counter = 0
    prGreen("\r\n" + "Checking for succesfully completed jobs:")
    time.sleep(2)
    for x in dataflowjobs_list:

        if x["jobType"] != "datasync" and x["status"] == "Success":
            counter += 1
            counter_2 += 1
            ex_date = x["executedDate"]
            ex_date = ex_date[:10]
            ex_time = x["executedDate"]
            ex_time = ex_time[11:19]
            print("{} - ".format(counter_2) ,"Job id:",x["id"],"- Status:",x["status"],"- Run Date:","{} {}".format(ex_date,ex_time),"- Label:",x["label"])

    if counter == 0:
        prYellow("\r\n" + "There are no completed Dataflows.")

    line_print()

    #Check all other jobs
    counter = 0
    prYellow("\r\n" + "Checking all other job types:")
    time.sleep(2)
    for x in dataflowjobs_list:

        if x["jobType"] != "datasync" and (x["status"] != "Success" and x["status"] != "Failure" and x["status"] != "Running"):
            counter += 1
            counter_2 += 1
            ex_date = x["executedDate"]
            ex_date = ex_date[:10]
            ex_time = x["executedDate"]
            ex_time = ex_time[11:19]
            print("{} - ".format(counter_2) ,"Job id:",x["id"],"- Status:",x["status"],"- Run Date:","{} {}".format(ex_date,ex_time),"- Label:",x["label"])



    if counter == 0:
        prYellow("\r\n" + "There are no available Dataflows.")

    line_print()


def get_datasyncJobs_list(dataflowjobs_list):
    counter_2 = 0

    #Check running jobs
    counter = 0
    prCyan("\r\n" + "Checking for jobs currently running:")
    time.sleep(2)
    for x in dataflowjobs_list:

        if x["jobType"] == "datasync" and x["status"] == "Running":
            counter += 1
            counter_2 += 1
            ex_date = x["executedDate"]
            ex_date = ex_date[:10]
            ex_time = x["executedDate"]
            ex_time = ex_time[11:19]
            print("{} - ".format(counter_2) ,"Job id:",x["id"],"- Status:",x["status"],"- Run Date:","{} {}".format(ex_date,ex_time),"- Label:",x["label"])

    if counter == 0:
        prYellow("\r\n" + "There are no Datasyncs running at the moment.")

    line_print()


    #Check failed jobs
    counter = 0
    prRed("\r\n" + "Checking for failed jobs:")
    time.sleep(2)
    for x in dataflowjobs_list:

        if x["jobType"] == "datasync" and x["status"] == "Failure":
            counter += 1
            counter_2 += 1
            ex_date = x["executedDate"]
            ex_date = ex_date[:10]
            ex_time = x["executedDate"]
            ex_time = ex_time[11:19]
            print("{} - ".format(counter_2) ,"Job id:",x["id"],"- Status:",x["status"],"- Run Date:","{} {}".format(ex_date,ex_time),"- Label:",x["label"])

    if counter == 0:
        prYellow("\r\n" + "There are no failed Datasyncs.")

    line_print()


    #Check successfull jobs
    counter = 0
    prGreen("\r\n" + "Checking for succesfully completed jobs:")
    time.sleep(2)
    for x in dataflowjobs_list:

        if x["jobType"] == "datasync" and x["status"] == "Success":
            counter += 1
            counter_2 += 1
            ex_date = x["executedDate"]
            ex_date = ex_date[:10]
            ex_time = x["executedDate"]
            ex_time = ex_time[11:19]
            print("{} - ".format(counter_2) ,"Job id:",x["id"],"- Status:",x["status"],"- Run Date:","{} {}".format(ex_date,ex_time),"- Label:",x["label"])

    if counter == 0:
        prYellow("\r\n" + "There are no completed Datasyncs.")

    line_print()

    #Check all other jobs
    counter = 0
    prYellow("\r\n" + "Checking all other status:")
    time.sleep(2)
    for x in dataflowjobs_list:

        if x["jobType"] == "datasync" and (x["status"] != "Success" and x["status"] != "Failure" and x["status"] != "Running"):
            counter += 1
            counter_2 += 1
            ex_date = x["executedDate"]
            ex_date = ex_date[:10]
            ex_time = x["executedDate"]
            ex_time = ex_time[11:19]
            print("{} - ".format(counter_2) ,"Job id:",x["id"],"- Status:",x["status"],"- Run Date:","{} {}".format(ex_date,ex_time),"- Label:",x["label"])
    if counter == 0:
        prYellow("\r\n" + "There are no other available Datasyncs.")

    line_print()


def get_AllJobs_list(dataflowjobs_list):
    counter_2 = 0


    #Check running jobs
    prGreen("\r\n" + "Checking for all job types:")
    counter = 0
    time.sleep(2)
    for x in dataflowjobs_list:
        counter += 1
        counter_2 += 1
        try:
            ex_date = x["executedDate"]
            ex_date = ex_date[:10]
            ex_time = x["executedDate"]
            ex_time = ex_time[11:19]
            print("{} - ".format(counter_2) ,"Job id:",x["id"],"- Status:",x["status"],"- Run Date:","{} {}".format(ex_date,ex_time),"- Label:",x["label"])
        except:
            print("{} - ".format(counter_2) ,"Job id:",x["id"],"- Status:",x["status"],"- Label:",x["label"])

    action_track = input("\r\nEnter a dataflow # (1 - {}) to examine the logs or hit any other key to go back: ".format(counter_2))

    counter_2 = 0

    counter_3 = 0

    try:
        action_track = int(action_track)
        if type(action_track) == int and action_track > 0 and action_track <= counter:
            for x in dataflowjobs_list:
                counter_2 += 1
                if counter_2 == action_track:
                    flow_id = x["id"]
            examine_dataflow(access_token,server_domain,flow_id)
    except:
        pass

    line_print()

    if counter == 0:
        prYellow("\r\n" + "There are no jobs available.")
        line_print()
