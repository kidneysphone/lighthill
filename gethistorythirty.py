# -*- coding: utf-8 -*-  
#coding=utf-8  
import tushare as ts
import pymongo as mongo
import pandas as pd
import json
import methods
import serverConfig

#c = mongo.MongoClient('mongodb://root:Liangdian123@dds-m5e0732db396e2e42.mongodb.rds.aliyuncs.com',3717);
#两地址
CONN_ADDR1 = 's-m5e9656f4072c8d4.mongodb.rds.aliyuncs.com:3717'
CONN_ADDR2 = 's-m5e7370f67573e34.mongodb.rds.aliyuncs.com:3717'
username = 'root'
password = 'Liangdian123'
#获取mongoclient
c = mongo.MongoClient([CONN_ADDR1, CONN_ADDR2])
#授权. 这里的user基于admin数据库授�?
c.admin.authenticate(username, password)
daily30 = c.daily30;

serverNo = serverConfig.serverNo;
serverCount = 8;
def getallcode():
    arr = []
    try:
        codea = c.db.codelist.find({'timeToMarket':{'$gt':0}}).sort('code')
        # print codea,'code':{'$gte':'000777','$lt':'002222'}
        for item in codea:
            arr.append(item['code'])
    except Exception,e:
        print Exception,":",e
    return arr
# def getcode(skip,needsolve):
#     arr = []
#     try:
#         codea = c.db.codelist.find({'timeToMarket':{'$gt':0}}).sort('code').skip(skip).limit(needsolve)
#         # print codea
#         for item in codea:
#             arr.append(item['code'])
#     except Exception,e:
#         print Exception,":",e
#     return arr
# def getneedsolve(isfirst=False,serverCount=10):
#     count = c.db.codelist.find({'timeToMarket':{'$gt':0}}).count()
#     ava = int(count/serverCount)
#     if(isfirst):
#         return ava + (count-ava*serverCount)
#     else:
#         return ava
# def getoffsets(serverNo):
#     firstSolve = 0;
#     if(serverNo==1):
#         needsolve = getneedsolve(True,serverCount)
#     else:
#         firstSolve = getneedsolve(True,serverCount)
#         needsolve = getneedsolve()
#     if(serverNo - 1>0):
#         offset1 = 1
#     else:
#         offset1 = 0
#     if(serverNo - 2>0):
#         offset2 = serverNo - 2
#     else:
#         offset2 = 0
#     return offset1,offset2,firstSolve
# offset1,offset2,firstSolve = getoffsets(serverNo)
# if(serverNo==1):
#     needsolve = getneedsolve(True,serverCount);
# else:
#     needsolve = getneedsolve(False,serverCount);
codelist = getallcode();
start = '2014-01-24';
end = '2017-02-13';
# if(serverNo==2):
#sh = ts.get_hist_data('sh',ktype='30',start=start,end=end);
#sz = ts.get_hist_data('sz',ktype='30',start=start,end=end);
#hs300 = ts.get_hist_data('hs300',ktype='30',start=start,end=end);
#sz50 = ts.get_hist_data('sz50',ktype='30',start=start,end=end);
#zxb = ts.get_hist_data('zxb',ktype='30',start=start,end=end);
#cyb = ts.get_hist_data('cyb',ktype='30',start=start,end=end);
#sh['date'] = sh['open'].keys();
#sz['date'] = sz['open'].keys();
#hs300['date'] = hs300['open'].keys();
#sz50['date'] = sz50['open'].keys();
#zxb['date'] = zxb['open'].keys();
#cyb['date'] = cyb['open'].keys();
#daily30['sh'].insert(json.loads(sh.to_json(orient='records')));
#daily30['sz'].insert(json.loads(sz.to_json(orient='records')));
#daily30['hs300'].insert(json.loads(hs300.to_json(orient='records')));
#daily30['sz50'].insert(json.loads(sz50.to_json(orient='records')));
#daily30['zxb'].insert(json.loads(zxb.to_json(orient='records')));
#daily30['cyb'].insert(json.loads(cyb.to_json(orient='records')));
#for code in codelist:
#    print code;    
#    tt = ts.get_hist_data(code,ktype='30',start=start,end=end);
#    if(not tt.empty):
#        tt['date'] = tt['open'].keys();
#        daily30[code].insert(json.loads(tt.to_json(orient='records')));

# step2 update original data
for code in codelist:
    collection = daily30[code];
    records = collection.find().sort([("date",mongo.ASCENDING)]);
   #{'date':{'$gte':'2017-01-24'}}
    for record in records:
        print code, record['date'];
        methods.updateRecord(code,record['date'],collection);
