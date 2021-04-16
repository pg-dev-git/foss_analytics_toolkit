import json
import requests
from terminal_colors import *
from sfdc_login import *
import math
import csv
import pandas as pd
import glob
import os
import base64
import threading
from dataset_tasks.dataset_extract_MP import *
import time

def mt_for_mp(mts,dataset_,dataset_currentVersionId,query_fields_str,q_offset,q_limit,i,access_token,dataset_name,server_id):
    for index in range(mts):
        x = threading.Thread(target=data_extract_mp, args=(dataset_,dataset_currentVersionId,query_fields_str,q_offset,q_limit,i,access_token,dataset_name,server_id,))
        threads.append(x)
        x.start()
        i += 1
        q_offset += 9999
        time.sleep(1)

    for index, thread in enumerate(threads):
        thread.join()
        time.sleep(1)
