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
import serverConfig
import httplib
CONN_ADDR1 = 's-m5e9656f4072c8d4.mongodb.rds.aliyuncs.com:3717'
CONN_ADDR2 = 's-m5e7370f67573e34.mongodb.rds.aliyuncs.com:3717'
username = 'root'
password = 'Liangdian123'
#获取mongoclient
c = mongo.MongoClient([CONN_ADDR1, CONN_ADDR2])
#授权. 这里的user基于admin数据库授�?
c.admin.authenticate(username, password)
codelist = cdb.db['codelist']
daily = cdb.daily30;
serverCount = 8;
serverNo = serverConfig.serverNo;
count = 0;
step = 30;
timer_interval = 30*60;
def savedaily30_quotes(code,thread):
    now = datetime.datetime.now();
    date = now.strftime("%Y-%m-%d");
    ctime = now.strftime("%H:%M:%S");
    if(ctime>'15:06:00'):
        exit();
    if(serverNo==2):
        daily['sh'].delete_many({'date':{'$gt':date}});
        daily['sz'].delete_many({'date':{'$gt':date}});
        daily['hs300'].delete_many({'date':{'$gt':date}});
        daily['sz50'].delete_many({'date':{'$gt':date}});
        daily['zxb'].delete_many({'date':{'$gt':date}});
        daily['cyb'].delete_many({'date':{'$gt':date}});
        sh = ts.get_hist_data('sh',ktype='30',start=date);
        sz = ts.get_hist_data('sz',ktype='30',start=date);
        hs300 = ts.get_hist_data('hs300',ktype='30',start=date);
        sz50 = ts.get_hist_data('sz50',ktype='30',start=date);
        zxb = ts.get_hist_data('zxb',ktype='30',start=date);
        cyb = ts.get_hist_data('cyb',ktype='30',start=date);
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
        collection = daily[c];
        try:
            last = daily[c].find({}).sort([("date", mongo.DESCENDING)]).limit(1);
            last = list(last);
            last = last[0];
            # print last['date'];
            d1 = datetime.datetime.strptime(last['date'], '%Y-%m-%d %H:%M:%S');
            d2 = d1 + datetime.timedelta(minutes=1);
            d3 = d2.strftime('%Y-%m-%d %H:%M:%S');
            # daily[c].delete_many({'date':{'$gt':date}});
            tt = ts.get_hist_data(c,ktype='30',start=d3);
            if(not tt.empty):
                print c,'tt not empty';
                #last = daily[c].find({}).sort([("date",mongo.DESCENDING)]).limit(1);
                #last = list(last)[0];

                tt['date'] = tt['open'].keys();
                #tt = tt[tt.date>last['date']];
                #if(not tt.empty):
                #    real = scdb.realtime['all'].find({'code':c,'date':date}).sort([("date",mongo.DESCENDING),("time",mongo.DESCENDING)]).limit(1);
                #    real = list(real);
                #    if(len(real)>0):
                #        tt['amount'] = list(real)[0]['amount']
                #daily[c].insert(json.loads(tt.to_json(orient='records')));
                #print c;
                        # stra1 = threading.Thread(target=update,args=(c,json.loads(tt.to_json(orient='records')),collection,));
                        # stra1.start();
                daily[c].insert(json.loads(tt.to_json(orient='records')));
                print c;

        except Exception,e:
            print Exception,e;
            # daily[c].delete_many({'date':{'$gt':date}});
            #tt = ts.get_hist_data(c,ktype='30',start=date);
            #if(not tt.empty):
            #    last = daily[c].find({}).sort([("date",mongo.DESCENDING)]).limit(1);
            #    last = list(last)[0];

             #   tt['date'] = tt['open'].keys();
              #  tt = tt[tt.date>last['date']];
              #  if(not tt.empty):
              #      real = scdb.realtime['all'].find({'code':c,'date':date}).sort([("date",mongo.DESCENDING),("time",mongo.DESCENDING)]).limit(1);
              #      real = list(real);
              #      if(len(real)>0):
              #          tt['amount'] = list(real)[0]['amount']
                    #daily[c].insert(json.loads(tt.to_json(orient='records')));
                        # stra1 = threading.Thread(target=update,args=(c,json.loads(tt.to_json(orient='records')),collection,));
                        # stra1.start();
    t1 = threading.Timer(timer_interval,savedaily30_quotes,(code,thread,));
    t1.start();

def update(records,collection):
    for record in records:
        method.updateRecord(record['code'],record['date'],collection);
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
def getneedsolve(isFirst=True,serverCount=10):
    count = codelist.find({'timeToMarket':{'$gt':0}}).count()
    ava = int(count/serverCount);
    if(serverNo==1):
        isfirst = True;
    else:
        isfirst = False;
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
        count += 1;
        needsolve -= step;
        t=threading.Timer(0,savedaily30_quotes,(code,count,));
        t.start();
    else:
        exit();

