# -*- coding: utf-8 -*-
#coding=utf-8
import tushare as ts
import pymongo as mongo
import json
import thread
import threading
import datetime
import time
import strategy
import httplib
import timeit
import traceback
import serverConfig
CONN_ADDR1 = 's-m5ee3f385a65d4d4.mongodb.rds.aliyuncs.com:3717'
#CONN_ADDR2 = 's-m5eb2244234d5944.mongodb.rds.aliyuncs.com:3717'
username = 'root'
password = 'Liangdian123'
#获取mongoclient
c = mongo.MongoClient([CONN_ADDR1])
#授权. 这里的user基于admin数据库授�?
c.admin.authenticate(username, password)
realtime = c.realtime;
codelist = c.db['codelist']
serverCount = 2;
serverNo = serverConfig.serverNo;
count = 0;
step = 30;
timer_interval = 0;
def saverealtime_quotes(code,thread):
    now = datetime.datetime.now();
    ctime = now.strftime("%H:%M:%S");
    if(ctime>'15:05:04'):
        exit();
    print (str(code) + " @ " + str(now))
    try:
        start=time.time()
        d = ts.get_realtime_quotes(code);

        # if(serverNo==2):
        #     sh = ts.get_realtime_quotes('sh');
        #     realtime['sh'].insert(json.loads(sh.to_json(orient='records')));
        #     sz = ts.get_realtime_quotes('sz');
        #     realtime['sz'].insert(json.loads(sz.to_json(orient='records')));
        #     hs300 = ts.get_realtime_quotes('hs300');
        #     realtime['hs300'].insert(json.loads(hs300.to_json(orient='records')));
        #     sz50 = ts.get_realtime_quotes('sz50');
        #     realtime['sz50'].insert(json.loads(sz50.to_json(orient='records')));
        #     hs300 = ts.get_realtime_quotes('hs300');
        #     realtime['hs300'].insert(json.loads(hs300.to_json(orient='records')));
        #     zxb = ts.get_realtime_quotes('zxb');
        #     realtime['zxb'].insert(json.loads(zxb.to_json(orient='records')));
        #     cyb = ts.get_realtime_quotes('cyb');
        #     realtime['cyb'].insert(json.loads(cyb.to_json(orient='records')));
        result = realtime['all'].insert(json.loads(d.to_json(orient='records')));
        conn = httplib.HTTPConnection("weixin.aitradeapp.com");
        conn.request('GET','/app/index.php?i=72&c=entry&do=util&m=ld_financial&op=price&p=update&d='+d.to_json(orient='records'));
        end=time.time()
        conn.close();
        #start=time.time() 时间测试
        strategy.strategy_00001(d);
        strategy.strategy_00002(d);
        strategy.strategy_00003(d);
        strategy.strategy_00004(d);
        strategy.strategy_00007(d);
        strategy.strategy_00008(d);
        strategy.strategy_00009(d);
        strategy.strategy_00010(d);
        strategy.strategy_00011(d);
        t1 = threading.Timer(timer_interval,saverealtime_quotes,(code,thread,));
        t1.start();
    except Exception,e:
        traceback.print_exc()
        t1 = threading.Timer(timer_interval,saverealtime_quotes,(code,thread,));
        t1.start();

def getcode(skip):
    arr = []
    try:
        codea = codelist.find({'timeToMarket':{'$gt':0}}).sort('code').skip(skip).limit(step);
        # print codea
        for item in codea:
            arr.append(item['code']);
    except Exception,e:
        traceback.print_exc()
    return arr;
def getneedsolve(isFirst=True,serverCount=2):
    count = codelist.find({'timeToMarket':{'$gt':0}}).count()#全部股票数量
    ava = int(count/serverCount); # 数量/服务器数量
    # print ("count:" + str(count) + "from getneedsolve()")
    # print ("serverCount" + str(serverCount) + "from getneedsove()")
    if(serverNo==1):
        isfirst = True;
    else:
        isfirst = False;
    if(isfirst):
        return ava + (count-ava*serverCount) #
    else:
        return ava
def getoffsets(serverNo):
    firstSolve = 0;
    if(serverNo==1):
        needsolve = getneedsolve(True,serverCount);
    else:
        firstSolve = getneedsolve(True,serverCount);
        needsolve = getneedsolve();
    if(serverNo - 1>0):
        offset1 = 1;
    else:
        offset1 = 0;
    if(serverNo - 2>0):
        offset2 = serverNo - 2;
    else:
        offset2 = 0;
    return offset1,offset2,firstSolve;
#执行主体

offset1,offset2,firstSolve = getoffsets(serverNo);
needsolve = getneedsolve(True,serverCount);
# print needsolve;
while(needsolve>0):
    now = datetime.datetime.now();
    date1 = now.strftime("%Y%m%d");
    conn = httplib.HTTPConnection("www.easybots.cn");
    conn.request('GET','/api/holiday.php?d='+date1)
    result = conn.getresponse();
    j = json.loads(result.read());
    conn.close();
    if(j[date1]=='0'):
        #start=time.time()
        code = getcode(count*step+offset1*firstSolve+offset2*needsolve);
        count += 1;
        needsolve -= step;
        t1=threading.Timer(0,saverealtime_quotes,(code,count,));
        t1.start();
        #end=time.time()
        #print(end-start)
    else:
        die();
