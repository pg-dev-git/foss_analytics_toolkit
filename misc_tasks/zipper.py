# importing required modules
from zipfile import ZipFile
import os
from misc_tasks.terminal_colors import *
import time
from misc_tasks.line import *

def get_all_file_paths(directory):

    file_paths = []

    for root, directories, files in os.walk(directory):
        for filename in files:
            filepath = os.path.join(root, filename)
            file_paths.append(filepath)

    return file_paths

def tcrm_zipper(directory,zip_name):
    file_paths = get_all_file_paths(directory)

    prGreen('\r\n' + 'Creating a Zip file for your backup...')
    line_print()
    time.sleep(0.1)

    with ZipFile('{}.zip'.format(zip_name),'w') as zip:
        for file in file_paths:
            zip.write(file)

    prGreen('\r\n' + 'All files zipped successfully!')
    line_print()
    time.sleep(0.1)
