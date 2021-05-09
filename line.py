import sys
import time

def line_print():
    time.sleep(0.1)
    print("\r\n")
    for i in range(50):
        sys.stdout.write('\r')
        sys.stdout.write('-'*i)
        sys.stdout.flush()
        time.sleep(0.0005)
    print("\r\n")
    time.sleep(0.1)
