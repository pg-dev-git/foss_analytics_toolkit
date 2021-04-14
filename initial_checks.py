import os


def init_folders():
    try:
        dataset_upload_dir = "tcrm_toolkit_data"
        os.mkdir(dataset_upload_dir)
    except OSError as error:
            print(" ")

    cd = os.getcwd()
    #print(cd)

    d_ext = "{}".format(cd)+"/tcrm_toolkit_data/"
    #print(d_ext)

    return d_ext
