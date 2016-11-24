# -*- coding: utf-8 -*-

import time
import re
import psutil
import subprocess
from socket import *

# get host network delay
def _get_delay(ip):
    time_str = []
    delay_time = 0
    for i in range(15):
        p = subprocess.Popen(["ping -c 1 "+ ip], stdin = subprocess.PIPE, stdout = subprocess.PIPE, stderr = subprocess.PIPE, shell = True)
        out = p.stdout.read()
        out_re = re.search((u'time=\d+\.+\d*'), out)
        if out_re is not None:
            time_tmp1 = filter(lambda x: x in '1234567890.', out_re.group())
            time_tmp2 = max(time_tmp1, 0)
            time_str.append(time_tmp2)
            delay_time = sum([float(x) for x in time_str]) / len(time_str)
    return round(delay_time , 2)

# get host's osd I/O load
def _get_io():
    devices = {}
    osd_io = {}
    # use regular expression matches the ceph disk and record
    partitions = psutil.disk_partitions()
    pattern = re.compile(r'/var/lib/ceph/osd/')
    # find device and it's index in partitions
    # result:{'sdb1': 4, 'sdc1': 5}
    for p in partitions:
        if pattern.match(p.mountpoint):
            devices_name = p.device[5:]
            devices[devices_name] = partitions.index(p)
    for key in devices:
        osd_num = partitions[devices[key]].mountpoint[23:]
        pre_read_bytes = psutil.disk_io_counters(perdisk=True)[key].read_bytes
        pre_write_bytes = psutil.disk_io_counters(perdisk=True)[key].write_bytes
        time.sleep(1)
        after_read_bytes = psutil.disk_io_counters(perdisk=True)[key].read_bytes
        after_write_bytes = psutil.disk_io_counters(perdisk=True)[key].write_bytes
        read_bytes = after_read_bytes - pre_read_bytes
        write_bytes = after_write_bytes - pre_write_bytes
        total_kbytes = (read_bytes + write_bytes)/1024
        osd_io[osd_num] = total_kbytes
    return osd_io


# send data
def _send_date():
    HOST = '172.25.1.11'
    PORT = 12345
    # BUFSIZE = 1024
    ADDR = (HOST, PORT)
    udpCliSock = socket(AF_INET, SOCK_DGRAM)

    while True:
        try:
            delay_tmp = _get_delay('172.25.1.254')
            delay = max(delay_tmp, 0)
            io = _get_io()
            cpu = max(psutil.cpu_percent(interval=1), 0.0)
            mem = max(psutil.virtual_memory().percent, 0.0)
            data = str((delay, cpu, mem, io))
            if not data:
                break
            udpCliSock.sendto(data, ADDR)
            time.sleep(10)
            # data,ADDR = udpCliSock.recvfrom(BUFSIZE)  #接受数据
            # if not data:
            #     break
            # print 'Server : ', data
        except Exception as  e:
            print ('Error: ', e)
    udpCliSock.close()

if __name__ == '__main__':
        _send_date()
