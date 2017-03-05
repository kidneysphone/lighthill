# -*- coding: utf-8 -*-
#coding=utf-8
import tushare as ts
import pymongo as mongo
import pandas as pd
import json
import methods
import serverConfig
import httplib
import time
#两地址
CONN_ADDR1 = 's-m5ee3f385a65d4d4.mongodb.rds.aliyuncs.com:3717'
#CONN_ADDR2 = 's-m5e7370f67573e34.mongodb.rds.aliyuncs.com:3717'
username = 'root'
password = 'Liangdian123'
#获取mongoclient
c = mongo.MongoClient([CONN_ADDR1])
#授权. 这里的user基于admin数据库授�?
c.admin.authenticate(username, password)
#c = mongo.MongoClient('mongodb://root:Liangdian123@dds-m5e0732db396e2e42.mongodb.rds.aliyuncs.com',3717)
daily = c.daily;
serverNo = serverConfig.serverNo;
serverCount = 8;
def getallcode():
    arr = []
    try:
        codea = c.db.codelist.find({'timeToMarket': {'$gt': 0}}).sort('code').limit(1)
        for item in codea:
            arr.append(item['code'])
    except Exception,e:
        print Exception,":",e
    return arr
#def getcode(skip,needsolve):
#    arr = []
#    try:
#        codea = c.db.codelist.find({'timeToMarket':{'$gt':0}}).sort('code').skip(skip).limit(needsolve)
#        # print codea
#        for item in codea:
#            arr.append(item['code'])
#    except Exception,e:
#        print Exception,":",e
#    return arr
#def getneedsolve(isfirst=False,serverCount=10):
#    count = c.db.codelist.find({'timeToMarket':{'$gt':0}}).count();
#    ava = int(count/serverCount);
#    if(isfirst):
#        return count-ava*(serverCount-1)
#    else:
#        return ava
#def getoffsets(serverNo):
#    firstSolve = 0;
#    if(serverNo==1):
#        needsolve = getneedsolve(True,serverCount)
#    else:
#        firstSolve = getneedsolve(True,serverCount)
#        needsolve = getneedsolve()
#    if(serverNo - 1>0):
#        offset1 = 1
#    else:
#        offset1 = 0
#    if(serverNo - 2>0):
#        offset2 = serverNo - 2
#    else:
#        offset2 = 0
#    return offset1,offset2,firstSolve
#offset1,offset2,firstSolve = getoffsets(serverNo)
#if(serverNo==1):
#    needsolve = getneedsolve(True,serverCount);
#else:
#    needsolve = getneedsolve(False,serverCount);
#print firstSolve,needsolve;
#codelist = getcode(offset1*firstSolve+offset2*needsolve,needsolve);
codelist=getallcode()

start = '2017-03-02';
end = '2017-03-02';
# if(serverNo==2):
sh = ts.get_hist_data('sh',start=start,end=end);
sz = ts.get_hist_data('sz',start=start,end=end);
hs300 = ts.get_hist_data('hs300',start=start,end=end);
sz50 = ts.get_hist_data('sz50',start=start,end=end);
zxb = ts.get_hist_data('zxb',start=start,end=end);
cyb = ts.get_hist_data('cyb',start=start,end=end);
sh['date'] = sh['open'].keys();
sz['date'] = sz['open'].keys();
hs300['date'] = hs300['open'].keys();
sz50['date'] = sz50['open'].keys();
zxb['date'] = zxb['open'].keys();
cyb['date'] = cyb['open'].keys();
daily['sh'].insert(json.loads(sh.to_json(orient='records')));
daily['sz'].insert(json.loads(sz.to_json(orient='records')));
daily['hs300'].insert(json.loads(hs300.to_json(orient='records')));
daily['sz50'].insert(json.loads(sz50.to_json(orient='records')));
daily['zxb'].insert(json.loads(zxb.to_json(orient='records')));
daily['cyb'].insert(json.loads(cyb.to_json(orient='records')));
for code in codelist:
     daily[code].remove({'date':'2017-03-02'})
     tt = ts.get_hist_data(code,ktype='D',start=start,end=end);
     print code;
     if(not tt.empty):
        tt['date'] = tt['open'].keys();
        daily[code].insert(json.loads(tt.to_json(orient='records')));
        time.sleep(1);
        tt1 = ts.get_k_data(code,start=start,end=end);
        print tt1
        if(tt1 is not None):
            tt1 = json.loads(tt1.to_json(orient='records'));
            for kdata in tt1:
                kdate = kdata['date'];
                record = daily[code].find_one({'date':kdate});
                if(record is not None):
                    high_qfq = kdata['high'];
                    low_qfq = kdata['low'];
                    open_qfq = kdata['open'];
                    close_qfq = kdata['close'];
                    #print record['date'],record;
                    daily[code].update({'_id':record['_id']},{'$set':{'high_qfq':high_qfq,'low_qfq':low_qfq,'open_qfq':open_qfq,'close_qfq':close_qfq}});
                    stri = "{";
                    for key, value in record.items():
                         stri = stri + "\"%s\":\"%s\"" % (key, value)+",";
                    stri = stri[0:len(stri)-1] + "}";
                    conn = httplib.HTTPConnection("weixin.aitradeapp.com");

                    result = conn.request('GET','/app/index.php?i=72&c=entry&do=util&m=ld_financial&op=daily&p=update&date='+record['date']+'&code='+code+'&data='+stri);
                    print result
                    conn.close();
#step2 update original data
#for code in codelist:
#    collection = daily[code];
#    records = collection.find({'date':{'$gte':'2017-01-19','$lt':'2017-01-20'}}).sort([("date",mongo.ASCENDING)]);
#    records = collection.find({}).sort([("date",mongo.ASCENDING)]);
#    for record in records:
#        print code, record['date'];
#        methods.updateRecord(code,record['date'],collection);
