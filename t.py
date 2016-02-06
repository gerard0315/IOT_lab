import mraa
import math
import time
import sys
import boto.dynamodb2
import thread
import threading
import select
import pyupm_i2clcd as lcd
from boto import kinesis
from  threading import Timer, Thread
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER

NEXT_STATE = 0

from Queue import Queue, Empty

def signal_safe_sleep(delay):
    q = Queue()
    try: q.get(True, delay)
    except Empty:
        pass

def tempData():
    tempSensor = mraa.Aio(1)
    a = tempSensor.read()
    R = 1023.0/a-1.0
    R = 100000.0*R
    logd = math.log(R/100000.0)
    Celsius = 1.0/(logd/4275+1/298.15)-273.15
    tempInC = round(Celsius, 2)
    TempString = str(tempInC)
    print TempString
    return TempString


def upload_db():
    data = tempData()
    print "db"
    #TO DO!!!!


def upload_kinesis():
    print "K"
    #TO DO!!!!


def check_status():
    global NEXT_STATE
    while True:
        if NEXT_STATE == 0:
            if switch.read():
                NEXT_STATE = 1
                #task_list.insert(0, 1) #test ues, ignore
                time.sleep(0.5) # 0.5 sec to to allow main thread update the NEXT_STATE para
            else:
                NEXT_STATE = 0
        elif NEXT_STATE == 1:
            if switch.read():
                NEXT_STATE = 0
                #task_list.insert(0, 0)
                time.sleep(0.5)
            else:
                NEXT_STATE = 1


if __name__ == "__main__":
    switch_pin_number=8
    switch = mraa.Gpio(switch_pin_number)
    switch.dir(mraa.DIR_IN)
    myLcd = lcd.Jhd1313m1(0, 0x3E, 0x62)
    myLcd.setColor(255, 255, 0)
    myLcd.setCursor(0, 0)
    myLcd.clear()
    print "ctrl+c to quit"
    statusKey = 0
    previous_status = 0

    #task_list = [0]
    flag = True
    check_thread= Thread(target = check_status)
    check_thread.setDaemon = True
    check_thread.start()


    try:
        while (flag):
            if NEXT_STATE == 0:
                print "next state is ", NEXT_STATE
                signal_safe_sleep(2)   #same as time.sleep()
            elif NEXT_STATE == 1:
                print "next state is ", NEXT_STATE
                signal_safe_sleep(2) 

    except KeyboardInterrupt:
                print "exiting"
                sys.exit()

    sys.exit()