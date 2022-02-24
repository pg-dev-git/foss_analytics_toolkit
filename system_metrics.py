from terminal_colors import *
import psutil
import os
from b2h import *

def get_sys_metrics_dn():
    iostat1 = psutil.net_io_counters(pernic=False)
    iostat1 = int(iostat1[1])
    cpu_usage = psutil.cpu_percent(1.3)
    ram_usage = psutil.virtual_memory()[2]
    iostat2 = psutil.net_io_counters(pernic=False)
    iostat2 = int(iostat2[1])
    speed_dn = iostat2 - iostat1
    speed_dn = bytes2human(speed_dn)

    print("CPU Usage:        ",end='')

    if cpu_usage >= 90 and cpu_usage <= 100:
        prLightPurple("{}%".format(cpu_usage))
    elif cpu_usage >= 75 and cpu_usage < 90:
        prLightPurple("{}%".format(cpu_usage))
    elif cpu_usage >= 50 and cpu_usage < 75:
        prCyan("{}%".format(cpu_usage))
    else:
        prYellow("{}%".format(cpu_usage))

    print("Ram Usage:        ",end='')

    if ram_usage >= 90 and ram_usage <= 100:
        prRed("{}%".format(ram_usage))
    elif ram_usage >= 75 and ram_usage < 90:
        prYellow("{}%".format(ram_usage))
    elif ram_usage >= 50 and ram_usage < 75:
        prLightPurple("{}%".format(ram_usage))
    else:
        prCyan("{}%".format(ram_usage))

    print("Download Speed:    {}/s".format(speed_dn))
