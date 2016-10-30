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

# get osd and its' 'REWEIGHT'
def get_osd_reweight_dic():
    global OSD_DUMP
    osd_reweight_dic = {}
    for osd_num in range(len(OSD_DUMP['osds'])):
        if ((OSD_DUMP['osds'][osd_num]['up']) and (OSD_DUMP['osds'][osd_num]['in'])) == 1:
            osd_reweight_temp = float(OSD_DUMP['osds'][osd_num]['weight'])
            osd_reweight_dic[OSD_DUMP['osds'][osd_num]['osd']] = osd_reweight_temp
            print ("get_osd_reweight: osd.%d reweight is %s" % (osd_num, osd_reweight_temp))
    return osd_reweight_dic

# get osd and its' bandwidth for <length> times
def get_osd_bw_dic(dict,length):
    pool = redis.ConnectionPool(host='172.25.1.2', port=6379, db=0)
    r = redis.StrictRedis(connection_pool=pool)
    osd_addr_dic = get_osd_addr_dic()

    #delete key in osd_bw but not in osd_addr_dic
    if dict:
        for key in dict.keys():
            if not osd_addr_dic.has_key(key):
                del dict[key]
    #update osd_bw of the osd in osd_addr_dic
    for key in osd_addr_dic.keys():
        if key not in dict:
            dict[key] = []
        bw = r.get(osd_addr_dic[key])
        dict[key].append(bw)
        if len(dict[key]) > length:
            dict[key].pop(0)

# calculate osd weight base on osd's bandwith
def calc_osd_weight(osd_bw_dic):
    osd_weight_dic = {}
    for osd in osd_bw_dic.keys():
        new_bw = float(float(osd_bw_dic[osd])/(10**6)) #new bandwidth (Mbit/s)
        weight = float(new_bw/1000) #0.0<weight<1.0
        osd_weight_dic[osd] = weight
    return osd_weight_dic

# execute 'ceph osd reweight'
def exec_osd_reweight(osd_weight,osd_reweight):
    for osd_num in osd_weight.keys():
        weight = osd_weight[osd_num]
        reweight = osd_reweight[osd_num] 
        if (weight < 0.8) and (reweight < 0.2):
            continue
        if (weight < 0.2) and (reweight == 1.0):
            cmd = "ceph osd reweight %s %s &" % (osd_num,weight)
            print("osd is %s, weight is %s "% (osd_num, weight))
            print ("ceph osd reweight: calling %s" % cmd)
            out = os.system(cmd)
            print ("osd_reweight: %s" % out)
        if (weight > 0.8) and (reweight != 1.0):
            cmd = "ceph osd reweight %s 1.0 &" % osd_num
            out = os.system(cmd)
            print ("osd_reweight to 1.0: %s" % out)

def main():
    times = 5
    osd_bw = {}
    while True:
        #update <times> times osd bandwidth,eg:times=5,osd_bw={'0':[1,2,3,4,5],'1':[6,7,8,9,10]}
        # evertime update's interval is 60s
        new_osd_bw = {}
        for i in range(times):
            update_osd_dump()
            get_osd_bw_dic(osd_bw,times)
            time.sleep(60)
            print(osd_bw)
        #calculate the average osd_bandwidth <times> times
        for key in osd_bw.keys():
            sum = 0.0
            for i in range(len(osd_bw[key])):
                sum = sum + float(osd_bw[key][i])
            new_bw = float(sum/len(osd_bw[key]))
            new_osd_bw[key] = new_bw
        print("new osd bandwidth is :")
        print(new_osd_bw)
        osd_weight = calc_osd_weight(new_osd_bw)
        print("osd weight is :")
        print (osd_weight)
        osd_reweight = get_osd_reweight_dic()
        exec_osd_reweight(osd_weight,osd_reweight)
        print("sleeping 30 minutes...")
        time.sleep(1800)

if __name__ == "__main__":
    main()
