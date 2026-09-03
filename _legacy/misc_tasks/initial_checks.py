import os, sys


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

def init_folders():
    try:
        dir = "toolkit_data"
        os.mkdir(dir)
    except OSError as error:
            print(" ")

    cd = os.getcwd()

    os_ = get_platform()

    if os_ == "Windows":
        d_ext = "{}".format(cd)+"\\toolkit_data\\"
    else:
        d_ext = "{}".format(cd)+"/toolkit_data/"

    return d_ext
