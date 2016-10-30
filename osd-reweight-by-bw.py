'''
    Introduction: The map includes osds in 'host' bucket and osd's bandwith,use CMD to reweight osd weight
                   base osd bandwith

    CMD:ceph osd reweight <int[0-]> <float[0.0-1.0]> :  reweight osd to 0.0 < <weight> < 1.0

    Environment: 1.redis:   apt-get install python-redis,
                            if client is not in server host, you should modify redis-server config
                            /etc/redis/redis.conf --> bind <really ip,not 127.0.0.1>

    Author:     KWJ(kyson)

    UpdateTime: 2016/10/28
'''

import os, sys
import re, commands, json
import time
import redis


# use 'ceph osd dump' to update osd info
def update_osd_dump():
    global OSD_DUMP
    osd_dump_json = commands.getoutput('ceph osd dump --format=json 2>/dev/null')
    OSD_DUMP = json.loads(osd_dump_json)
    print ("update_osd_dump: done")

# get osd and its' physical address
def get_osd_addr_dic():
    global OSD_DUMP
    osd_addr_dic = {}
    for osd_num in range(len(OSD_DUMP['osds'])):
        if ((OSD_DUMP['osds'][osd_num]['up'])and(OSD_DUMP['osds'][osd_num]['in']))==1:
            osd_addr_temp = OSD_DUMP['osds'][osd_num]['cluster_addr']
            osd_addr = re.findall(r'(?<![\.\d])(?:\d{1,3}\.){3}\d{1,3}(?![\.\d])', osd_addr_temp)[0]
            osd_addr_dic[OSD_DUMP['osds'][osd_num]['osd']]=osd_addr
            print ("get_osd_addr: osd.%d address is %s" % (osd_num, osd_addr))
    return osd_addr_dic

# get osd and its' bandwidth
def get_osd_bw_dic():
    osd_bw_dic = {}
    pool = redis.ConnectionPool(host='172.25.1.2', port=6379, db=0)
    r = redis.StrictRedis(connection_pool=pool)
    osd_addr_dic = get_osd_addr_dic()
    for key in osd_addr_dic.keys():
        osd_bw = r.get(osd_addr_dic[key])
        osd_bw_dic[key] = osd_bw
    return osd_bw_dic

# calculate osd weight base on osd's bandwith
def calc_osd_weight(osd_bw_dic):
    osd_weight_dic = {}
    for osd in osd_bw_dic.keys():
        new_bw = float(float(osd_bw_dic[osd])/(10**6)) #new bandwidth (Mbit/s)
        weight = float(new_bw/1000) #0.0<weight<1.0
        osd_weight_dic[osd] = weight
    return osd_weight_dic

# execute 'ceph osd reweight'
def osd_reweight(osd_num,weight):
    if (weight < 0.2) :
        cmd = "ceph osd reweight %s %s &" % (osd_num,weight)
        print("osd is %s, weight is %s "% (osd_num, weight))
        print ("ceph osd reweight: calling %s" % cmd)
        out = os.system(cmd)
        print ("osd_reweight: %s" % out)
#
def main():
    while True:
        update_osd_dump()
        osd_bw = get_osd_bw_dic()
        osd_weight = calc_osd_weight(osd_bw)
        for osd_num in osd_weight.keys():
            osd_reweight(osd_num,osd_weight[osd_num])
        print("sleeping half an hour...")
        time.sleep(5)

if __name__ == "__main__":
    main()
