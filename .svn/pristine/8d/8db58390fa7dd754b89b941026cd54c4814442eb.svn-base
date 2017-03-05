# -*- coding: utf-8 -*-
#coding=utf-8
import tushare as ts
import pymongo as mongo
import json
import thread
import threading
import datetime
import time
import methods
import strategy
import httplib
import serverConfig
CONN_ADDR1 = 's-m5ee3f385a65d4d4.mongodb.rds.aliyuncs.com:3717'
#CONN_ADDR2 = 's-m5e7370f67573e34.mongodb.rds.aliyuncs.com:3717'
username = 'root'
password = 'Liangdian123'
#获取mongoclient
c = mongo.MongoClient([CONN_ADDR1])
#授权. 这里的user基于admin数据库授�?
c.admin.authenticate(username, password)
codelist = c.db['codelist']
daily = c.daily;
serverCount = 2;
serverNo = serverConfig.serverNo;
count = 0;
step = 30;

def savedaily_quotes(code,thread):
    now = datetime.datetime.now().date();
    date = str(now);

    if(serverNo==2):
        sh = ts.get_hist_data('sh',start=date);
        sz = ts.get_hist_data('sz',start=date);
        hs300 = ts.get_hist_data('hs300',start=date);
        sz50 = ts.get_hist_data('sz50',start=date);
        zxb = ts.get_hist_data('zxb',start=date);
        cyb = ts.get_hist_data('cyb',start=date);
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
    for c in code:
        # print (c)
        collection = daily[c];
        try:
            tt = ts.get_hist_data(c,ktype='D',start=date, end = date);
            if(not tt.empty):
                tt['date'] = date;
                daily[c].insert(json.loads(tt.to_json(orient='records')));
                temp = json.loads(tt.to_json(orient='records'));
                stri = "";
                #print (type(temp))
                #print (temp)
                for key, value in temp[0].items():
                    stri = stri + "\"%s\":\"%s\"" % (key, value) + ",";
                    # print ("(key, value) = " + str(key) + " , " + str(value))
                stri = stri[0:len(stri) - 1] + "}";
                conn = httplib.HTTPConnection("weixin.aitradeapp.com");
                #print (str(date) + ": " + stri)

                result = conn.request('GET','/app/index.php?i=72&c=entry&do=util&m=ld_financial&op=daily&p=update&date='+date+'&code='+c+"&data="+stri);
                conn.close();
                # methods.caclExprice(c,date);
                record = collection.find({}).sort([("date",mongo.DESCENDING)]).limit(1);
                # print (record)
                # stra1 = threading.Thread(target=updateRecord,args=(c,date,collection,));
                # stra1.start();

                # stra5 = threading.Thread(target=strategy.strategy_00005,args=(list(record)[0],));
                # stra5.start();
            except Exception,e:
                print ("got exception")
                tt = ts.get_hist_data(c,ktype='D',start=date);
                if(not tt.empty):
                    tt['date'] = date;
                    tt1 = ts.get_h_data(c,start=date,end=date);
                if(tt1 is not None):
                    tt['amount'] = tt1['amount'].values[0];
                else:
                    tt['amount'] = None;
                # daily[c].insert(json.loads(tt.to_json(orient='records')));
                conn = httplib.HTTPConnection("weixin.aitradeapp.com");

                result = conn.request('GET','/app/index.php?i=72&c=entry&do=util&m=ld_financial&op=daily&p=update&date='+date+'&code='+c);
                # methods.caclExprice(c,date);
                record = collection.find({}).sort([("date",mongo.DESCENDING)]).limit(1);
                # stra1 = threading.Thread(target=updateRecord,args=(c,date,collection,));
                # stra1.start();

                # stra5 = threading.Thread(target=strategy.strategy_00005,args=(list(record)[0],));
                # stra5.start();

def getcode(skip):
    arr = []
    try:
        codea = codelist.find({'timeToMarket':{'$gt':0}}).sort('code').skip(skip).limit(step)
        # print codea
        for item in codea:
            arr.append(item['code'])
    except Exception,e:
        print Exception,":",e
    return arr
def getneedsolve(isfirst=False,serverCount=2):
    count = codelist.find({'timeToMarket':{'$gt':0}}).count()
    ava = int(count/serverCount)
    if(isfirst):
        return ava + (count-ava*serverCount)
    else:
        return ava
def getoffsets(serverNo):
    firstSolve = 0;
    if(serverNo==1):
        needsolve = getneedsolve(True,serverCount)
    else:
        firstSolve = getneedsolve(True,serverCount)
        needsolve = getneedsolve()
    if(serverNo - 1>0):
        offset1 = 1
    else:
        offset1 = 0
    if(serverNo - 2>0):
        offset2 = serverNo - 2
    else:
        offset2 = 0
    return offset1,offset2,firstSolve
offset1,offset2,firstSolve = getoffsets(serverNo)
if(serverNo==1):
    needsolve = getneedsolve(True,serverCount);
else:
    needsolve = getneedsolve(False,serverCount);
while(needsolve>0):
    now = datetime.datetime.now();
    date1 = now.strftime("%Y%m%d");
    conn = httplib.HTTPConnection("www.easybots.cn");
    conn.request('GET','/api/holiday.php?d='+date1);
    result = conn.getresponse();
    j = json.loads(result.read());
    conn.close();
    if(j[date1]=='0'):
        code = getcode(count*step+offset1*firstSolve+offset2*needsolve)
        #print code
        count += 1;
        needsolve -= step;
#        t=threading.Timer(0,savedaily_quotes,(code,count,));
 #       t.start();
    else:
        exit();
