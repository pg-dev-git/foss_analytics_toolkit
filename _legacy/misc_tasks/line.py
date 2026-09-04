import sys, time

def line_print():
    time.sleep(0.005)
    print("\r\n")
    for i in range(50):
        sys.stdout.write('\r')
        sys.stdout.write('-'*i)
        sys.stdout.flush()
        time.sleep(0.00001)
    print("\r\n")
    time.sleep(0.005)

def line_print_ext():
    time.sleep(0.0005)
    print("\r\n")
    for i in range(100):
        sys.stdout.write('\r')
        sys.stdout.write('-'*i)
        sys.stdout.flush()
        #time.sleep(0.0000001)
    print("\r\n")
    time.sleep(0.0005)
