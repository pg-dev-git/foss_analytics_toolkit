# importing required modules
from zipfile import ZipFile
import os
from terminal_colors import *
import time
from line import *

def get_all_file_paths(directory):

    # initializing empty file paths list
    file_paths = []

    # crawling through directory and subdirectories
    for root, directories, files in os.walk(directory):
        for filename in files:
            # join the two strings in order to form the full filepath.
            filepath = os.path.join(root, filename)
            file_paths.append(filepath)

    # returning all file paths
    return file_paths

def tcrm_zipper(directory,zip_name):
    # path to folder which needs to be zipped
    #directory = './python_files'

    # calling function to get all file paths in the directory
    file_paths = get_all_file_paths(directory)

    # printing the list of all files to be zipped
    prGreen('\r\n' + 'Creating a Zip file for your backup...')
    line_print()
    time.sleep(0.1)

    #for file_name in file_paths:
    #    print(file_name)

    # writing files to a zipfile
    with ZipFile('{}.zip'.format(zip_name),'w') as zip:
        # writing each file one by one
        for file in file_paths:
            zip.write(file)

    prGreen('\r\n' + 'All files zipped successfully!')
    line_print()
    time.sleep(0.1)
