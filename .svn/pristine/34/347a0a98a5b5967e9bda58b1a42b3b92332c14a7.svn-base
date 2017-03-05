# -*- coding: utf-8 -*-  �?
#coding=utf-8  
import tushare as ts
import pymongo as mongo
import pandas as pd
import json
import methods
import serverConfig
import httplib
import datetime;
#两地址
CONN_ADDR1 = 's-m5ee3f385a65d4d4.mongodb.rds.aliyuncs.com:3717'
#CONN_ADDR2 = 's-m5e7370f67573e34.mongodb.rds.aliyuncs.com:3717'
username = 'root'
password = 'Liangdian123'
#获取mongoclient
c = mongo.MongoClient([CONN_ADDR1])
#授权. 这里的user基于admin数据库授�?
c.admin.authenticate(username, password)
daily5 = c.daily5;
serverNo = serverConfig.serverNo;
serverCount = 8;
def getallcode():
    arr = []
    try:
        codea = c.db.codelist.find({'timeToMarket':{'$gt':0},'code':{'$gte':'000012'}}).sort('code')
        # print codea ,'code':{'$gte':'000777','$lt':'002222'},'code':{'$gte':'002109'}
        for item in codea:
            arr.append(item['code'])
    except Exception,e:
        print Exception,":",e
    #print(arr)
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
start = '2017-03-02';
end = '2017-03-03';
# if(serverNo==2):
# sh = ts.get_hist_data('sh',ktype='5',start=start,end=end);
# sz = ts.get_hist_data('sz',ktype='5',start=start,end=end);
# hs300 = ts.get_hist_data('hs300',ktype='5',start=start,end=end);
# sz50 = ts.get_hist_data('sz50',ktype='5',start=start,end=end);
# zxb = ts.get_hist_data('zxb',ktype='5',start=start,end=end);
# cyb = ts.get_hist_data('cyb',ktype='5',start=start,end=end);
# sh['date'] = sh['open'].keys();
# sz['date'] = sz['open'].keys();
# hs300['date'] = hs300['open'].keys();
# sz50['date'] = sz50['open'].keys();
# zxb['date'] = zxb['open'].keys();
# cyb['date'] = cyb['open'].keys();
# daily5['sh'].insert(json.loads(sh.to_json(orient='records')));
# daily5['sz'].insert(json.loads(sz.to_json(orient='records')));
# daily5['hs300'].insert(json.loads(hs300.to_json(orient='records')));
# daily5['sz50'].insert(json.loads(sz50.to_json(orient='records')));
# daily5['zxb'].insert(json.loads(zxb.to_json(orient='records')));
# daily5['cyb'].insert(json.loads(cyb.to_json(orient='records')));
for code in codelist:
     print code
     tt = ts.get_hist_data(code,ktype='5',start=start,end=end);
     daily5[code].remove();
     if(not tt.empty):
        tt['date'] = tt['open'].keys();
        print code;
        daily5[code].insert(json.loads(tt.to_json(orient='records')));
        tt1 = ts.get_k_data(code,ktype='5',start=start,end=end);
        if(tt1 is not None):
            tt1 = json.loads(tt1.to_json(orient='records'));
            for kdata in tt1:
                kdate = kdata['date'];
                kdate = str(kdate)+":00";
                record = daily5[code].find_one({'date':kdate});
                if(record is not None): 
                    high_qfq = kdata['high'];
                    low_qfq = kdata['low'];
                    open_qfq = kdata['open'];
                    close_qfq = kdata['close'];
                    daily5[code].update({'_id':record['_id']},{'$set':{'high_qfq':high_qfq,'low_qfq':low_qfq,'open_qfq':open_qfq,'close_qfq':close_qfq}});
                    stri = "{";
                    for key, value in record.items():
                        stri = stri + "\"%s\":\"%s\"" % (key, value) + ",";
                    stri = stri[0:len(stri) - 1] + "}";
                    conn = httplib.HTTPConnection("weixin.aitradeapp.com");

                    result = conn.request('GET','/app/index.php?i=72&c=entry&do=util&m=ld_financial&op=daily5&p=update&code=' + code + '&data=' + stri + '&date=' + kdate);
                    result = conn.getresponse();
                    conn.close();
#step2 update original data
#for code in codelist:
#    collection = daily5[code];
#    records = collection.find({'date':{'$gte':'2017-01-24','$lt':'2017-01-26'}}).sort([("date",mongo.ASCENDING)]);
#    records = list(records);
#    if(len(records)<1):
#        continue;
#    for record in records:
#        print code,record['date'];
#        methods.updateRecord(code,record['date'],collection);
# # step3 超额十字星数据初始化
for code in codelist:
    collection = daily5[code];
    records = collection.find().sort([("date",mongo.ASCENDING)]);
    records = list(records);
    code = code.encode("utf-8");
    if(code.startswith('00') and code.startswith('002')):
        grail = daily5['zxb'];
    elif(code.startswith('00')):
        grail = daily5['sz'];
    elif(code.startswith('60')):
        grail = daily5['sh'];
    elif(code.startswith('30')):
        grail = daily5['cyb'];
    if len(records) == 0:
        continue;
    else:
        base = records[0]['open'];
    count = 0;
    for record in records:
        if(count<1):
            collection.update({'_id':record['_id']},{'$set':{'exprice':base}});
        else:
            grecord = grail.find_one({'date':record['date']});
            if(grecord is None):
                p_change = 0;
            else:
                p_change= grecord ['p_change'];
            exp_change = record['p_change'] - p_change;
            base = base * (1+exp_change);
            collection.update({'_id':record['_id']},{'$set':{'exprice':base}});
        count += 1;
# step4 超额十字�?最高�?开盘�?最低�?收盘�?

for code in codelist:
    collection = c.daily[code];
    excollection = c.daily5[code];
    records = collection.find().sort([("date",mongo.ASCENDING)]);
    for record in records:
        exrecords = excollection.find({'date':{'$gte':record['date']+' 09:00:00','$lte':record['date']+' 15:30:00'}});
        exrecords = list(exrecords);
        if(len(exrecords)<1):
            continue;
        high = exrecords[0]['exprice'];
        low = exrecords[0]['exprice'];
        exopen = exrecords[0]['exprice'];
        exclose = exrecords[0]['exprice'];
        count = 1;
        for exr in exrecords:
            if(count == 1):
                exopen = exr['exprice'];
            if(count == len(exrecords)):
                exclose = exr['exprice'];
            if(exr['exprice']>high):
                high = exr['exprice'];
            if(exr['exprice']<low):
                low = exr['exprice'];
            count = count + 1;
        cdaily = c.daily[code].find_one({'date':record['date']});
        c.daily[code].update({'_id':cdaily['_id']},{'$set':{'exopen':exopen,'exclose':exclose,'exhigh':high,'exlow':low}});
