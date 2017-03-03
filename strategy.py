# -*- coding: utf-8 -*-  
#coding=utf-8
import httplib
import pymongo as mongo
CONN_ADDR1 = 's-m5ee3f385a65d4d4.mongodb.rds.aliyuncs.com:3717'
#CONN_ADDR2 = 's-m5eb2244234d5944.mongodb.rds.aliyuncs.com:3717'
username = 'root'
password = 'Liangdian123'
#获取mongoclient
c = mongo.MongoClient([CONN_ADDR1])
#授权. 这里的user基于admin数据库授�?
c.admin.authenticate(username, password)
history = c.daily5;

# 策略00001 买点
# 现价离MA5 上方 0.05% 提示买入
def strategy_00001(data):
    date = data['date'].values;
    date = date[0];
    codes = data['code'].values;
    prices = data['price'].values;
    count = 0;
    for code in codes:
        lastRecord = list(history[code].find({'date':{'$lt':date}}).sort([("date",mongo.DESCENDING)]).limit(1))[0];
        ma5 = lastRecord['ma5'];
        price = float(prices[count]);
        if(((price-ma5)/ma5)>(0.05/100)):
            conn = httplib.HTTPConnection("weixin.aitradeapp.com");
            conn.request('GET','/app/index.php?i=72&c=entry&do=strategy&m=ld_financial&serial=00001&type=1&once=1&code='+code);
            conn.close();
        count += 1;

# 策略00002 买点
# 现价离MA10 上方0.05% 提示买入
def strategy_00002(data):
    date = data['date'].values;
    date = date[0];
    codes = data['code'].values;
    prices = data['price'].values;
    count = 0;
    for code in codes:
        lastRecord = list(history[code].find({'date':{'$lt':date}}).sort([("date",mongo.DESCENDING)]).limit(1))[0];
        ma10 = lastRecord['ma10'];
        price = float(prices[count]);
        if(((price-ma10)/ma10)>(0.05/100)):
            conn = httplib.HTTPConnection("weixin.aitradeapp.com");
            conn.request('GET','/app/index.php?i=72&c=entry&do=strategy&m=ld_financial&serial=00002&type=1&once=1&code='+code);
            conn.close();
        count += 1;

# 策略00003 买点
# 10日内上涨10%以上 且现价price在ma5上方0.05% 提示买入
def strategy_00003(data):
    date = data['date'].values;
    date = date[0];
    codes = data['code'].values;
    prices = data['price'].values;
    count = 0;
    for code in codes:
        records = list(history[code].find({'date':{'$lt':date}}).sort([("date",mongo.DESCENDING)]).limit(10));
        if(len(records)<10):
            count += 1;
            continue;
        lastRecord = records[0];
        lastRecordten = records[9];
        ma5 = lastRecord['ma5'];
        closeTen = lastRecordten['close'];
        price = float(prices[count]);
        if((price-closeTen)/closeTen>0.1 and ((price-ma5)/ma5)>(0.05/100)):
            conn = httplib.HTTPConnection("weixin.aitradeapp.com");
            conn.request('GET','/app/index.php?i=72&c=entry&do=strategy&m=ld_financial&serial=00003&type=1&once=1&code='+code);
            conn.close();
        count += 1;

# 策略00004 买点
# 10日内下跌 10% 以上时 前天收盘价<前天开盘价0.98 同时 昨日开盘价<前天收盘价 同时 昨日收盘价>前天开盘价时 price<昨天收盘价*1.02 提示买入
def strategy_00004(data):
    date = data['date'].values;
    date = date[0];
    codes = data['code'].values;
    prices = data['price'].values;
    count = 0;
    for code in codes:
        records = list(c.daily[code].find({'date':{'$lt':date}}).sort([("date",mongo.DESCENDING)]).limit(10));
        if(len(records)<10):
            return;
        lastRecord = records[0];
        lastRecord1 = records[1];
        lastRecordten = records[9];
        ma5 = lastRecord['ma5'];
        closeTen = lastRecordten['close'];
        price = float(prices[count]);
        if((price-closeTen)/closeTen<-0.1 and lastRecord1['close']<0.98*lastRecord1['open'] and lastRecord['open']<lastRecord['close'] and lastRecord['close']>lastRecord1['open'] and price<1.02*lastRecord['close']):
            conn = httplib.HTTPConnection("weixin.aitradeapp.com");
            conn.request('GET','/app/index.php?i=72&c=entry&do=strategy&m=ld_financial&serial=00004&type=1&once=1&code='+code);
            conn.close();
        count += 1;

# 策略00005 买点
# 十字星出现时 提示买入
def strategy_00005(record):
    h = abs((record['exopen']-record['exclose']));
    s = record['exhigh'] - max(record['exopen'],record['exclose']);
    x = min(record['exopen'],record['exclose']) - record['exlow'];
    if(h<0.1/100 and s>3*0.1/100 and x>3*0.1/100):
        if(record['open']-record['amount']/record['volume']<0.01):
            conn = httplib.HTTPConnection("weixin.aitradeapp.com");
            conn.request('GET','/app/index.php?i=72&c=entry&do=strategy&m=ld_financial&serial=00005&type=1&once=0&code='+code);
            conn.close();

# 策略00006 买点
# 当均线排列 由 任何排列情况 转变为ma5>ma10>ma20>ma60 提示买入
def strategy_00006(data):
    date = data['date'].values;
    date = date[0];
    codes = data['code'].values;
    count = 0;
    prices = data['price'].values;
    for code in codes:
        records = list(history[code].find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(2));
        if(records[0]['ma5']>records[0]['ma10'] and records[0]['ma10']>records[0]['ma20'] and records[0]['ma20']>records[0]['ma60']):
            if(records[1]['s7'] == 0):
                conn = httplib.HTTPConnection("weixin.aitradeapp.com");
                conn.request('GET','/app/index.php?i=72&c=entry&do=strategy&m=ld_financial&serial=00006&type=1&once=1&code='+code);
                conn.close();
            history[code].update({'_id':records[0]['_id']},{'$set':{'s7':1}});
        else:
            history[code].update({'_id':records[0]['_id']},{'$set':{'s7':0}});
        count += 1;

# 策略00007 买点
# close 连续3日在ma5之上时 ma5*1.001<price时
def strategy_00007(data):
    date = data['date'].values;
    date = date[0];
    codes = data['code'].values;
    prices = data['price'].values;
    count = 0;
    for code in codes:
        price = float(prices[count]);
        records = list(history[code].find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(3));
        if(records[0]['close']>records[0]['ma5'] and records[1]['close']>records[1]['ma5'] and records[2]['close']>records[2]['ma5'] and price>1.001*records[0]['ma5']):
            conn = httplib.HTTPConnection("weixin.aitradeapp.com");
            conn.request('GET','/app/index.php?i=72&c=entry&do=strategy&m=ld_financial&serial=00007&type=1&once=1&code='+code);
            conn.close();
        count += 1;

# 策略00008 卖点
# ma5（实时）>price and ma10(实时）>price
def strategy_00008(data):
    date = data['date'].values;
    date = date[0];
    prices = data['price'].values;
    codes = data['code'].values;
    count = 0;
    for code in codes:
        price = float(prices[count]);
        record = list(history[code].find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(1))[0];
        if(record['ma5']>price and record['ma10']>price):
            conn = httplib.HTTPConnection("weixin.aitradeapp.com");
            conn.request('GET','/app/index.php?i=72&c=entry&do=strategy&m=ld_financial&serial=00008&type=2&once=1&code='+code);
            conn.close();
        count += 1;

# 策略00009 卖点
# 跳空跌破ma10 open>close*1.001 时price<ma10
def strategy_00009(data):
    date = data['date'].values;
    
    date = date[0];

    codes = data['code'].values;
    prices = data['price'].values;
    count = 0;
    for code in codes:
        price = float(prices[count]);
        record = list(history[code].find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(1))[0];
        if(record['open']>1.001*record['close'] and price <record['ma10']):
            conn = httplib.HTTPConnection("weixin.aitradeapp.com");
            conn.request('GET','/app/index.php?i=72&c=entry&do=strategy&m=ld_financial&serial=00009&type=2&once=1&code='+code);
            conn.close();
        count += 1;

# 策略000010 卖点
# 距最高价回落3% price< high*0.97 提醒卖出
def strategy_00010(data):
    date = data['date'].values;
    date = date[0];
    codes = data['code'].values;
    prices = data['price'].values;
    count = 0;
    for code in codes:
        price = float(prices[count]);
        record = list(history[code].find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(1))[0];
        if(price < record['high']*0.97):
            conn = httplib.HTTPConnection("weixin.aitradeapp.com");
            conn.request('GET','/app/index.php?i=72&c=entry&do=strategy&m=ld_financial&serial=00010&type=2&once=1&code='+code);
            conn.close();
        count += 1;

# 策略000011 卖点
# 开盘20分钟以后 price<实时均价 卖出
def strategy_00011(data):
    date = data['date'].values;
    date = date[0];
    d = date.split(' ')[0];
    if(data['time'].values[0]>'09:50:00'):
        codes = data['code'].values;
        prices = data['price'].values;
        volumes = data['volume'].values;
        amounts = data['amount'].values;
        count = 0;
        for code in codes:
            price = float(prices[count]);
            volume = float(volumes[count]);
            amount = float(volumes[count]);
            if(volume>0 and price < float(amount)/volume):
                conn = httplib.HTTPConnection("weixin.aitradeapp.com");
                conn.request('GET','/app/index.php?i=72&c=entry&do=strategy&m=ld_financial&serial=00011&type=2&once=1&code='+code);
                conn.close();
            count += 1;