# -*- coding: utf-8 -*-  
#coding=utf-8  
import tushare as ts
import pymongo as mongo
import json
import thread
from threading import Timer
import datetime
import time
import pandas as pd
import urllib2
import httplib 
import math
import numpy as np 
#两地址
CONN_ADDR1 = 'dds-m5e0732db396e2e42.mongodb.rds.aliyuncs.com:3717'
CONN_ADDR2 = 'dds-m5e0732db396e2e41.mongodb.rds.aliyuncs.com:3717'
REPLICAT_SET = 'mgset-2881031'
username = 'root'
password = 'Liangdian123'
#获取mongoclient
c = mongo.MongoClient([CONN_ADDR1, CONN_ADDR2], replicaSet=REPLICAT_SET)
#授权. 这里的user基于admin数据库授�?
c.admin.authenticate(username, password)


# CloseSD, 收盘价标准差
# ma: 收盘价均�?
# records: n日的收盘�?
# return sd
def getCloseSD(ma,records):
    sumDelta = 0;
    for item in records:
        if(item['close'] is not None):
            delta = item['close'] - ma;
            sumDelta += delta*delta;
    return math.sqrt(sumDelta/len(records));

# TR: |当日最高价-当日最低价|�?|前个交易日收盘加-当日最高价|，|前个交易日收盘加-当日最低价|�?三值中的最大�?
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return tr
def caclTR(code,date,collection):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(2);
    records = list(records);
    if(len(records)<2):
        collection.update({'_id':records[0]['_id']},{'$set':{'tr':None}});
        return None;
    else:
        var1 = abs(records[0]['high']-records[0]['low']);
        var2 = abs(records[1]['close']-records[0]['high']);
        var3 = abs(records[1]['close']-records[0]['low']);
        maxvar = max(max(var1,var2),var3);
        collection.update({'_id':records[0]['_id']},{'$set':{'tr':maxvar}});
        return maxvar;

# ATR6  ATR6(1)= （当日TR + 前一日TR…�?�?日TR�?6
#      ATR6(i)= （ATR6(i-1) *5+TR�?6
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# loop: 递归次数 相当于ATR6(i)中的i
# return atr6
def caclATR6loop(code,date,collection,loop):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(6);
    records = list(records);
    if(len(records)<6 or not records[5].has_key('tr') or records[5]['tr'] is None):
        return None;
    sumTR = 0;
    for item in records:
        if(item['tr'] is not None):
            sumTR += item['tr'];
    preATR = sumTR/6;
    atr = preATR;
    count = 1;
    while(count<loop):
        atr = (5*preATR + records[0]['tr'])/6;
        preATR = atr;
        count += 1;
    return atr;

# ATR14  ATR14(1)= （当日TR + 前一日TR…�?�?3日TR�?14
#       ATR14(i)= （ATR14(i-1) *13+TR�?14
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return atr14
def caclATR14loop(code,date,collection,loop):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(14);
    records = list(records);
    if(len(records)<14 or not records[13].has_key('tr') or records[13]['tr'] is None):
        return None;
    sumTR = 0;
    for item in records:
        sumTR += item['tr'];
    preATR = sumTR/14;
    atr = preATR;
    count = 1;
    while(count<loop):
        atr = (13*preATR + records[0]['tr'])/14;
        preATR = atr;
        count += 1;
    return atr;

# BIAS5 BIAS5=（收盘价-5日收盘价平均值）/5日收盘价平均�?100
#       BIA10=（收盘价-10日收盘价平均值）/10日收盘价平均�?100
#       BIA20=（收盘价-20日收盘价平均值）/20日收盘价平均�?100
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return bias5,bias10,bias20
def caclBIAS(code,date,collection):
    record = collection.find_one({'date':date});
    if(record is None):
        # collection.update({'_id':record['_id']},{'$set':{'bias5':None,'bias10':None,'bias20':None}});
        return None,None,None;
    if(record['ma5']==0):
        bias5 = None;
    else:
        bias5 = (record['close'] - record['ma5'])/record['ma5']*100;
    if(record['ma10']==0):
        bias10 = None;
    else:
        bias10 = (record['close'] - record['ma10'])/record['ma10']*100;
    if(record['ma20']==0):
        bias20 = None;
    else:
        bias20 = (record['close'] - record['ma20'])/record['ma20']*100;
    collection.update({'_id':record['_id']},{'$set':{'bias5':bias5,'bias10':bias10,'bias20':bias20}});
    return bias5,bias10,bias20;


# CloseEMA 计算收盘价的N日的EMA值，一个公用的方法
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# loop: N日内
# return ema12, ema26...
def caclCloseEMA(code,date,collection,loop):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(loop);
    records = list(records);
    if(len(records)<loop):
        collection.update({'_id':records[0]['_id']},{'$set':{'ema'+str(loop):None}});
        return None;
    total = loop;
    denominator = (1+loop)*loop/2;
    ema = 0;
    sumClose = 0;
    total = loop;
    for item in records:
        if(item['close'] is not None):
            ema += (float(loop)/denominator) * float(item['close']);
            sumClose += item['close'];
        loop -= 1;
    collection.update({'_id':records[0]['_id']},{'$set':{'ema'+str(total):ema}});
    return ema;


# MA60 计算收盘价的60日的均�?
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return ma60    
def caclMA60(code,date,collection,):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(60);
    records = list(records);
    if(len(records)<60):
        collection.update({'_id':records[0]['_id']},{'$set':{'ma60':None}});
        return None;
    sumMa = 0;
    for item in records:
        if(item['close'] is not None):
            sumMa += float(item['close'])/60;
    collection.update({'_id':records[0]['_id']},{'$set':{'ma60':sumMa}});
    return sumMa;

# BIAS60 BIAS=(EMA(收盘价，N)-MA(收盘�?M))/MA(收盘价，M)*100;
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return bias60    
def caclBIAS60(code,date,collection):
    ema60 = caclCloseEMA(code,date,collection,60);
    ma60 = caclMA60(code,date,collection);
    if(ema60 is None or ma60 is None or ma60==0):
        return None;
    else:
        return float(ema60-ma60)/ma60*100;

# STD20 20日内收盘价的标准�?getCloseSD;
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return std20   
def caclSTD20(code,date,collection):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(20);
    records = list(records);
    if(len(records)<20):
        return None;
    ma = records[0]['ma20'];
    return getCloseSD(ma,records);

# BOLLUP MA20+2*20日内收盘价的标准差STD(CLOSE,20) 
# BOLLDOWN MA20-2*20日内收盘价的标准差STD(CLOSE,20)
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return bollup, bolldown
def caclBOLLUPDOWN(code,date,collection):
    record = collection.find_one({'date':date});
    std20 = caclSTD20(code,date,collection,);
    if(record is None or std20 is None or record['ma20'] is None):
        return None,None;
    else:
        return record['ma20']+2*std20,record['ma20']-2*std20;

# TP （最高价+最低价+收盘价）/ 3 
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return tp 
def caclTP(code,date,collection):
    record = collection.find_one({'date':date});
    if(record['high'] is None  or  record['low'] is None  or  record['close'] is None):
        collection.update({'_id':records[0]['_id']},{'$set':{'tp':None}});
        return None;
    collection.update({'_id':record['_id']},{'$set':{'tp':(record['high']+record['low']+record['close'])/3}});
    return (record['high']+record['low']+record['close'])/3;

# MA88 88日收盘价均�?
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return ma88     
def caclMA88(code,date,collection):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(88);

    records = list(records);
    if(len(records)<88):
        collection.update({'_id':records[0]['_id']},{'$set':{'ma88':None}});
        # return None;
    sumClose = 0;
    for item in records:
        if(item['close'] is not None):
            sumClose += item['close'];
    collection.update({'_id':records[0]['_id']},{'$set':{'ma88':sumClose/88}});
    # return sumClose/88;

# CCI(N) (TP-MA) /AVEDEV（TP,N�?0.015 
# AVEDEV(TP,5)=（|�?天的TP-MA|+|�?日的TP - MA| +……|第一天的TP- MA|�?5
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# duration: N值， �?�?0�?0�?8
# return cciN
def caclCCI(code,date,collection,duration):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(duration);
    records = list(records);
    if(len(records)<duration or not records[duration-1].has_key('ma'+str(duration)) or records[duration-1]['ma'+str(duration)] is None):
        return None;
    sumDelta = 0;
    for item in records:
        if(item['tp'] is not None and item['ma'+str(duration)] is not None):
            sumDelta += abs(item['tp']-item['ma'+str(duration)]);
    if(sumDelta<=0):
        return None;
    avedev = float(sumDelta)/duration;
    if(avedev==0):
        return None;
    return float(records[0]['tp']-records[0]['ma'+str(duration)])/avedev/0.015;

# RSV (当日收盘�?9日内最低价)/�?日内最高价-9日内最低价�?100
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return rsv
def caclRSV(code,date,collection):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(9);
    records = list(records);
    if(len(records)<9):
        collection.update({'_id':records[0]['_id']},{'$set':{'rsv':None}});
        return None;
    llv = records[0]['low'];
    hhv = records[0]['high'];
    cRecord = records[0];

    for item in records:
        if(item['low'] is not None):
            llv = min(item['low'],llv);
        if(item['high'] is not None):
            hhv = max(item['high'],hhv);
    if((hhv-llv)==0):
        collection.update({'_id':records[0]['_id']},{'$set':{'rsv':None}});
        return None;
    else:
        collection.update({'_id':records[0]['_id']},{'$set':{'rsv':(cRecord['close']-llv)/(hhv-llv)*100}});
        return (cRecord['close']-llv)/(hhv-llv)*100;

# K SMA(RSV,M1,1) M1=3
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# loop: M1
# return k
def caclRSVSMA(code,date,collection,loop):
    record = collection.find_one({'date':date});
    if(record['rsv'] is None):
        collection.update({'_id':record['_id']},{'$set':{'k':None}});
        return None;
    else:
        temp = collection.find({'date':{'$lt':date}}).sort([("date",mongo.DESCENDING)]).limit(loop);
        temp = list(temp);
        if(len(temp)<loop or not temp[loop-1].has_key('rsv') or temp[loop-1]['rsv'] is None ):
            collection.update({'_id':record['_id']},{'$set':{'k':None}});
            return None;
        else:
            sumK = record['rsv'];
            for item in temp:
                if(item['rsv'] is not None):
                    sumK+=item['rsv'];
            collection.update({'_id':record['_id']},{'$set':{'k':float(1)/(loop+1)*sumK}});
            return float(1)/(loop+1)*sumK;

# 前一天的K值，若无前天K值则�?0 
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return prevK
def getPrevK(code,date,collection):
    prev = collection.find({'date':{'$lt':date}}).sort([("date",mongo.DESCENDING)]).limit(1);
    prev = list(prev);
    if(prev is None or len(prev)<1 or not prev[0].has_key('k') or prev[0]['k'] is None):
        return 50;
    else:
        return prev[0]['k'];

# KDJ_K: RSV当天/3 + 2/3*前一天K�?
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return kdj_k
def caclKDJ_K(code,date,collection):
    record = collection.find_one({'date':date});
    if(not record.has_key('rsv') or record['rsv'] is None):
        return None;
    rsv = record['rsv'];
    if(rsv is None):
        return None;
    prevK = getPrevK(code,date,collection,);
    return float(rsv)/3+float(2)/3*prevK;

# D: SMA(K,M2,1) M2=3
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# loop: M2
# return d    
def caclKSMA(code,date,collection,loop):
    record = collection.find_one({'date':date});
    if(record['k'] is None):
        collection.update({'_id':record['_id']},{'$set':{'d':None}});
        return None;
    else:
        temp = collection.find({'date':{'$lt':date}}).sort([("date",mongo.DESCENDING)]).limit(loop);
        temp = list(temp);
        if(len(temp)<loop or temp[loop-1]['k'] is None):
            collection.update({'_id':record['_id']},{'$set':{'d':None}});
            return None;
        else:
            sumK = record['k'];
            for item in temp:
                if(item['k'] is not None):
                    sumK += item['k'];
            collection.update({'_id':record['_id']},{'$set':{'d':float(1)/(loop+1)*sumK}});
            return float(1)/(loop+1)*sumK;

# 前一天的D值，若无前天D值则�?0 
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return prevD  
def getPrevD(code,date,collection):
    prev = collection.find({'date':{'$lt':date}}).sort([("date",mongo.DESCENDING)]).limit(1);

    prev = list(prev);
    if(prev is None or len(prev)<1 or not prev[0].has_key('d') or prev[0]['d'] is None):
        return 50;
    else:
        return prev[0]['d'];

# KDJ_D: 当日K�?3+前一天D�?/3
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return kdj_d
def caclKDJ_D(code,date,collection):
    record = collection.find_one({'date':date});
    k = record['k'];
    if(k is None):
        return None;  
    prevD = getPrevD(code,date,collection);
    return float(k)/3+float(2)/3*prevD;

# ROC N: 100*(CLOSE-REF(CLOSE,N))/REF(CLOSE,N)
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# duration: 偏移�?这里�?�?1�?4�?0
# return roc    
def caclROC(code,date,collection,duration):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(duration);
    records = list(records);
    if(len(records)<duration):
        collection.update({'_id':records[0]['_id']},{'$set':{'roc'+str(duration):None}});
        return None;
    else:
        cur = records[0];
        prev = records[duration-1];
        if(prev['close']==0 or prev['close'] is None):
            collection.update({'_id':records[0]['_id']},{'$set':{'roc'+str(duration):None}});
            return None;
        collection.update({'_id':records[0]['_id']},{'$set':{'roc'+str(duration):100*float(cur['close']-prev['close'])/prev['close']}});
        return 100*float(cur['close']-prev['close'])/prev['close'];

# SBM: 暂无公式
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return sbm         
def caclSBM(code,date,collection):
    # TODO
    return;

# STM: 暂无公式
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return stm 
def caclSTM(code,date,collection):
    # TODO
    return

# USD: 今日收盘�?昨日收盘�?
#          �?USD= 10日内收盘价的标准�?
#          则DSD=0
#      今日收盘�?昨日收盘�?
#          则DSD=10日内收盘价的标准�?
#          则USD=0 
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return usd,dsd 
def caclUSDDSD(code,date,collection):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(2);

    records = list(records);
    if(len(records)<2):
        collection.update({'_id':records[0]['_id']},{'$set':{'usd':None,'dsd':None}});
        return None,None;
    itemClose = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(10);     
    itemClose = list(itemClose);
    if(len(itemClose)<10):
        collection.update({'_id':records[0]['_id']},{'$set':{'usd':None,'dsd':None}});
        return None;
    sd = getCloseSD(records[0]['ma10'],list(itemClose));
    if(records[0]['close']<=records[1]['close']):
        collection.update({'_id':records[0]['_id']},{'$set':{'usd':0,'dsd':sd}});
        return 0,sd;
    else:
        collection.update({'_id':records[0]['_id']},{'$set':{'usd':sd,'dsd':0}});
        return sd,0;

# UPRVI:  10日内USD的WSMA�?
#         WSMA1=sum（收盘价�?0�?10
#         WSMA(i)=( sum(前十�?-wsma1+当日收盘�?/10
# DOWNRVI:  10日内DSD的WSMA�?
#           WSMA1=sum（收盘价�?0�?10
#           WSMA(i)=( sum(前十�?-wsma1+当日收盘�?/10
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return uprvi,downrvi,rvi         
def caclUPDOWNRVI(code,date,collection):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(10);
    records = list(records);
    if(len(records)<10 or not records[9].has_key('usd') or records[9]['usd'] is None or records[9]['dsd'] is None):
        return None,None,None;
    else:
        sumUsd = 0;
        sumDsd = 0
        for record in records:
            if(record['usd'] is not None):
                sumUsd += record['usd'];
            if(record['dsd'] is not None):
                sumDsd += record['dsd'];
        uprvi = (sumUsd-records[0]['ma10']+records[0]['close'])/10;
        downrvi = (sumDsd-records[0]['ma10']+records[0]['close'])/10;
        if(uprvi+downrvi==0):
            return uprvi,downrvi,None;
        rvi = 100*uprvi/(uprvi+downrvi);
    return uprvi,downrvi,rvi;

# SRMI: 暂无公式
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return srmi 
def caclSRMI(code,date,collection):
    # TODE
    return;

# ChandeSD:  日收盘价与做日收盘价（下跌日）差值的绝对值加总。若当日上涨，则增加值为0�?
#            CZ2:=IF(CLOSE-REF(CLOSE,1)<0,ABS(CLOSE-REF(CLOSE,1)),0);
#            SD:=SUM(CZ2,N);
# ChandeSU:  今日收盘价与昨日收盘价（上涨日）差值加总。若当日?��跌，则增加值为0
#            CZ1:=IF(CLOSE-REF(CLOSE,1)>0,CLOSE-REF(CLOSE,1),0);
#            SU:=SUM(CZ1,N);
# CMO: (SU-SD)/(SU+SD)*100;
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# duration: 相当于N
# return chandeSD,chandeSU,cmo     
def caclChandeSDSUCMO(code,date,collection,duration):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(duration+1);
    records = list(records);
    if(len(records)<duration+1 or records[duration]['close'] is None):
        return None,None,None;
    else:
        count = 0;
        sumChangeSD = 0;
        sumChangeSU = 0;
        while(count<duration):
            if(records[count]['close']<records[count+1]['close']):
                sumChangeSD += abs(records[count]['close']-records[count+1]['close']);
            elif(records[count]['close']>records[count+1]['close']):
                sumChangeSU += abs(records[count]['close']-records[count+1]['close']);
            count += 1;
        if(sumChangeSD+sumChangeSU==0):
            return sumChangeSD,sumChangeSU,None;
        return sumChangeSD,sumChangeSU,(sumChangeSD-sumChangeSU)/(sumChangeSD+sumChangeSU);

# BIASDIF: (BIAS-REF(BIAS,M));//BIAS减M个周期前的BIAS;
# BIAS:=(CLOSE-MA(CLOSE,N))/MA(CLOSE,N);//收盘价减收盘价在N个周期内的简单移动平均比收盘价在N个周期内的简单移动平均；
# bias5可由Line 92公式得出
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# N=5,M=16
# return diff 
def caclBIASDIF(code,date,collection):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(16);
    records = list(records);
    if(len(records)<16):
        collection.update({'_id':records[0]['_id']},{'$set':{'diff':None}});
        return None;
    else:
        if(records[15]['bias5'] is None):
            collection.update({'_id':records[0]['_id']},{'$set':{'diff':None}});
            return None;
        collection.update({'_id':records[0]['_id']},{'$set':{'diff':records[0]['bias5']-records[15]['bias5']}});
        return records[0]['bias5']-records[15]['bias5'];

# DBCD: (SMA(DIF,T,1);//DIF在T个周期内的移动平�?
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# loop: T�?
# return dbcd 
def caclDBCD(code,date,collection,loop):
    record = collection.find_one({'date':date});
    records = collection.find({'date':{'$lt':date}}).sort([("date",mongo.DESCENDING)]).limit(loop);
    records = list(records);
    if(len(records)<loop or not records[loop-1].has_key('diff') or records[loop-1]['diff'] is None):
        return None;
    else:
        sumDiff = record['diff'];
        for record in records:
            if(record['diff'] is not None):
                sumDiff += record['diff'];
        return float(1)*sumDiff/(loop+1);

# ARC: 暂无公式
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return arc 
def caclARC(code,date,collection):
    # TODO
    return;

# OBV: OBV = OBV昨日+T*交易�?
#      如果收盘�?昨日收盘�?
#           T=1
#      如果收盘�?昨日收盘�?
#           T=-1
#      如果收盘�?昨日收盘�?
#           T=0
#      若无昨日OBV数据，则使用昨日交易�?
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return obv 
def caclOBV(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(2);

    records = list(records);
    if(len(records)<2):
        collection.update({'_id':records[0]['_id']},{'$set':{'obv':None}});

        return None;
    else:
        if(records[1]['obv'] is None):
            obv = records[1]['volume'];
        else:
            obv = records[1]['obv'];
        if(records[0]['close']>records[1]['close']):
            plu = records[0]['volume'];
        elif(records[0]['close']==records[1]['close']):
            plu = 0;
        else:
            plu = -records[0]['volume'];
        collection.update({'_id':records[0]['_id']},{'$set':{'obv':obv + plu}});
        return obv + plu;
# OBVN: OBVN=sum（OBV, N�? N
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# duration: 周期，这里取6,20
# return obvmaN 
def getOBVMA(code,date,collection,duration,):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(duration);
    records = list(records);
    if(len(records)<duration or not records[duration-1].has_key('obv') or records[duration-1]['obv'] is None):
        return None;
    sumObv = 0;
    for record in records:
        if(record['obv'] is not None):
            sumObv += record['obv'];
    if(len(records)==0):
        return None;
    return sumObv/len(records);
# TVMA6: 6日成交金额的移动平均值（Turnover Value Moving Average�?
#       TVMA6=SUM( 成交金额�?) / 6 
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return tvma6 
def caclTVMA6(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(6);
    records = list(records);
    if(len(records)<6):
        collection.update({'_id':records[0]['_id']},{'$set':{'tvma6':None}});
        return None;
    else:
        sumTV = 0;
        for record in records:
            if(record.has_key('amount') and record['amount'] is not None ):
                sumTV += float(record['amount']);
        collection.update({'_id':records[0]['_id']},{'$set':{'tvma6':sumTV/6}});
        return sumTV/6;
# TVMA20: 20日成交金额的移动平均值（Turnover Value Moving Average�?
#       TVMA20=SUM( 成交金额�?0) / 20 
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return tvma20 
def caclTVMA20(code,date,collection):
    # collection = c.history[code]
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(20);
    records = list(records);
    if(len(records)<20):
        collection.update({'_id':records[0]['_id']},{'$set':{'tvma20':None}});
        return None;
    else:
        sumTV = 0;
        for record in records:
            if(record.has_key('amount') and record['amount'] is not None):
                sumTV += float(record['amount']);
        collection.update({'_id':records[0]['_id']},{'$set':{'tvma20':sumTV/20}});
        return sumTV/20;

# TVSTD6: 6日成交金额的标准差（Turnover Value Standard Deviation）�?
#         TVSTD6=STD( 成交金额, 6) 
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return tvstd6 
def caclTVSTD6(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(6);
    records = list(records);
    if(len(records)<6 or not records[0].has_key('amount') or records[0]['amount'] is None):
        return None;
    else:
        sumTVSTD = 0;
        for record in records:
            if(not record.has_key('amount') or record['amount'] is None):
                record['amount'] = 0;
            delta = records[0]['tvma6'] - float(record['amount']);
            sumTVSTD += delta * delta;
        return math.sqrt(sumTVSTD/6);

# TVSTD20: 20日成交金额的标准差（Turnover Value Standard Deviation）�?
#         TVSTD20=STD( 成交金额, 20) 
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return tvstd20 
def caclTVSTD20(code,date,collection):
    # collection = c.history[code];'
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(20);
    records = list(records);
    if(len(records)<20 or not records[19].has_key('amount') or records[19]['amount'] is None):
        return None;
    else:
        sumTVSTD = 0;
        for record in records:
            if(record['amount'] is None):
                record['amount'] = 0;
            delta = records[0]['tvma6'] - float(record['amount']);
            sumTVSTD += delta * delta;
        return math.sqrt(sumTVSTD/20);

# VDIFF: VSHORT－VLONG
#        VSHORT=[2×成交�?（N1�?）×上一周期成交量]
#        VLONG=[2×成交�?（N2�?）×上一周期成交量]
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# N1=12 N2=26
# return vdiff 
def caclVDIFF(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(26);
    records = list(records);
    if(len(records)<26 or records[0]['volume'] is None):
        collection.update({'_id':records[0]['_id']},{'$set':{'vdiff':None}});
        return None;
    else:
        count = 0;
        shorts = 0;
        longs = 0;
        while(count<26):
            if(records[count]['volume'] is not None):
                if(count<12):
                    shorts += records[count]['volume'];
                longs += records[count]['volume'];
            count+=1;
        vshort = 2*records[0]['volume']+(12-1)*shorts;
        vlong = 2*records[0]['volume']+(26-1)*longs;
        collection.update({'_id':records[0]['_id']},{'$set':{'vdiff':vshort - vlong}});
        return vshort - vlong;
# VDEA: EMA(VDIFF,M);
# VMACD: VMACD= VDIFF �?VDEA 
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return vdea,vmacd 
def caclVDEAVMACD(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(9);
    records = list(records);
    if(len(records)<9 or not records[8].has_key('vdiff') or records[8]['vdiff'] is None):
        return None,None;
    else:
        denominator = (1+9)*9/2;
        ema = 0;
        loop = 9;
        for item in records:
            if(item['vdiff'] is not None):
                ema += (float(9)/denominator) * float(item['vdiff']);
            loop -= 1;
        return ema,records[0]['vdiff']-ema;

# VOSC: VOSC=(VSHORT-VLONG) / VSHORT *100
#       VSHORT=n日周期中成交量的总和/n
#       VLONG= m日周期中成交量的总和/ m
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# n=12,m=26
# return vosc        
def caclVOSC(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(26);
    records = list(records);
    if(len(records)<26):
        return None;
    else:
        count = 0;
        shorts = 0;
        longs = 0;
        while(count<26):
            if(records[count]['volume'] is not None):
                if(count<12):
                    shorts += records[count]['volume'];
                longs += records[count]['volume'];
            count += 1;
        vshort = float(shorts)/12;
        vlong = float(longs)/26;
        if(vshort == 0):
            return None;
        return (vshort-vlong)/vshort*100;

# VR: VR=（AVS+1/2CVS�?（BVS+1/2CVS�?
#     1．n天以来凡是股价上涨那一天的成交量都称为AV，将24天内的AV总和相加后称为AVS�?
#     2．n天以来凡是股价下跌那一天的成交量都称为BV，将24天内的BV总和相加后称为BVS�?
#     3．n天以来凡是股价不涨不跌，则那一天的成交量都称为CV，将24天内的CV总和相加后称为CVS
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# n=24
# return vr 
def caclVR(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(25);
    records = list(records);
    if(len(records)<25):
        return None;
    else:
        avs = 0;
        bvs = 0;
        cvs = 0;
        count = 0;
        while(count<24):
            if(records[count]['close']>records[count+1]['close'] and records[count]['volume'] is not None):
                avs += records[count]['volume'];
            elif(records[count]['close']<records[count+1]['close'] and records[count]['volume'] is not None):
                bvs += records[count]['volume'];
            elif(records[count]['close']==records[count+1]['close'] and records[count]['volume'] is not None):
                cvs += records[count]['volume'];
            count += 1;
        if(bvs+float(cvs)/2 == 0):
            return None;
        return float(avs+float(cvs)/2)/(bvs+float(cvs)/2);

# VROC6=（成交量－N日前的成交量）÷N日前的成交量×100
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# N=6
# return vroc6        
def caclVROC6(code,date,collection):
    # collection = c.history[code];
    record = collection.find_one({'date':date});
    preR = collection.find({'date':{'$lt':date}}).sort([("date",mongo.DESCENDING)]).skip(5).limit(1);
    preR = list(preR);
    if(len(preR)<1  or  record['volume'] is None  or  preR[0]['volume'] is None):
        return None;
    else:
        if(preR[0]['volume'] == 0):
            return None;
        return float(record['volume']-preR[0]['volume'])/preR[0]['volume']*100;

# VROC12=（成交量－N日前的成交量）÷N日前的成交量×100
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# N=12
# return vroc12
def caclVROC12(code,date,collection):
    # collection = c.history[code];
    record = collection.find_one({'date':date});
    preR = collection.find({'date':{'$lt':date}}).sort([("date",mongo.DESCENDING)]).skip(11).limit(1);
    preR = list(preR);
    if(len(preR)<1  or  record['volume'] is None  or  preR[0]['volume'] is None):
        return None;
    else:
        if(preR[0]['volume'] == 0):
            return None;
        return float(record['volume']-preR[0]['volume'])/preR[0]['volume']*100;

# VSTD10: 10日成交量标准�?
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return vstd10
def caclVSTD10(code,date,collection):
    # collection = c.history[code];
    record = collection.find_one({'date':date});
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(10);
    records = list(records);
    if(len(records)<10  or  record['v_ma10'] is None):
        return None;
    else:
        sumVS = 0;
        for item in records:
            if(item['volume'] is None):
                item['volume'] = 0;
            delta = item['volume'] - record['volume'];
            sumVS += delta*delta;
        vs = sumVS/10;
        return math.sqrt(vs);

# VSTD20: 20日成交量标准�?
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return vstd20
def caclVSTD20(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(20);
    records = list(records);
    record = records[0];
    if(len(records)<20  or  record['v_ma10'] is None):
        return None;
    else:
        sumVS = 0;
        for item in records:
            if(item['volume'] is None):
                item['volume'] = 0;
            delta = item['volume'] - record['v_ma10'];
            sumVS += delta*delta;
        vs = sumVS/20;
        return math.sqrt(vs);

# CM: SUM(DM,M);
# records: 股票数据
# fast: 短周�?
# slow: 长周�?
# return cmfast,cmslow
def getCM(records,fast,slow):
    cmfast = 0;
    cmslow = 0;
    count = 0;
    for item in records:
        if(item['high'] is None or item['low'] is None):
            dm = 0;
        else:
            dm = item['high'] - item['low'];
        if(count<fast):
            cmfast += dm ;
        cmslow += dm;
    return cmfast,cmslow;

# VOLMOV: VOL*ABS(2*(DM/CMM)-1)*T*100;
# crecord: 当前股票信息
# records: 股票数据
# return vmf,vms        
def caclVOLMOV(crecord,records,collection):
    cmfast,cmslow = getCM(records,34,55);
    dm = crecord['high'] - crecord['low'];
    if(records[0]['tp']>records[1]['tp']):
        t = 1;
    else:
        t = -1;
    if(cmfast==0):
        volmovfast = None;
    else:
        volmovfast = crecord['volume']*2*abs(float(dm)/cmfast-1)*t*100;
    if(cmslow==0):
        volmovslow = None;
    else:
        volmovslow = crecord['volume']*2*abs(float(dm)/cmslow-1)*t*100;
    # return attribute vmf,vms
    collection.update({'_id':crecord['_id']},{'$set':{'vmf':volmovfast,'vms':volmovslow}});

    # return volmovfast,volmovslow;

# VOLMOVEMA: EMA(VOLMOVN,N)
# records: 股票数据
# return emafast,emaslow 
def getVOLMOVEMA(records):
    emafast = 0;
    emaslow = 0;
    countfast = 34;
    countslow = 55;
    denominatorfast = (1+34)*34/2;
    denominatorslow = (1+55)*55/2;
    for item in records:
        if(countfast>0):
            if(item['vmf'] is not None):
                emafast += float(countfast)/denominatorfast*item['vmf'];
        if(item['vms'] is not None):
            emaslow += float(countslow)/denominatorslow*item['vms'];
        countfast -= 1;
        countslow -= 1;
    return emafast,emaslow;

# KO: (EMA(VOLMOVM,M)-EMA(VOLMOVN,N))/100000;
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return ko
def caclKO(code,date,collection):
    records = collection.find({'date':{"$lte":date}}).sort([("date",mongo.DESCENDING)]).limit(55);
    records = list(records);
    crecord = records[0];
    if(len(records)>54):
        caclVOLMOV(crecord,records,collection);
    if(len(records)<55 or not records[54].has_key('vms') or records[54]['vms'] is None):
        return None;
    
    records = collection.find({'date':{"$lte":date}}).sort([("date",mongo.DESCENDING)]).limit(55);
    records = list(records);
    emafast,emaslow = getVOLMOVEMA(records);
    return float(emafast - emaslow)/100000;

# MoneyFlow20: 资金流量（MF�?TP×成交�?
# Tp= （high + low + close�?3
# 1、分辨PMF及NMF�?
# 2、如果当日MF>昨日MF，则将当日的MF值视为当日的PMF值，而当日NMF�?0
# 3、如果当日MF<昨日MF，则将当日的MF值视为当日的NMF值，而当日PMF�?0
# 4. Mr= sum（pmf�?0�?sum（nmf,20�?
# 5、MFI=100-(100÷�?+MR））
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# duration: 周期，这里取20
# return mfi
def caclMFI(code,date,collection,duration):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(duration+1);
    records = list(records);
    if(len(records)<duration+1):
        return None;
    else:
        pmf = 0;
        nmf = 0;
        count = len(records);
        mr = 0;
        while(count<duration):
            tomf = records[count]['tp'] * records[count]['volume'];
            yemf = records[count+1]['tp'] * records[count+1]['volume'];
            if(tomf>yemf):
                pmf += tomf;
            elif(tomf<yemf):
                nmf += tomf;
            count += 1;
        if(nmf>0):
            mr = float(pmf)/nmf;
        if(mr>0):
            return 100-100/float(1+mr);
        else:
            return None;
# AD: A/D line＝昨天的A/D值＋（收盘价位置常数*成交量）
#     收盘价位置常�?((收盘价－最低价)�?最高价－收盘价))/(最高价－收盘价)
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return ad
def caclAD(code,date,collection):
    records = collection.find({"date":{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(2);
    records = list(records);
    if(len(records)<2):
        collection.update({'_id':records[0]['_id']},{'$set':{'ad':None}});
        return None;
    else:
        if(records[1]['ad'] is None):
            ad = 0;
        else:
            ad = records[1]['ad'];
        cconts = 0;
        high = records[0]['high'];
        low = records[0]['low'];
        close = records[0]['close'];
        if(high-close==0):
            cconts = 0;
        else:
            cconts = float((close-low)-(high-close))/(high-close);
        collection.update({'_id':records[0]['_id']},{'$set':{'ad':ad + (cconts*records[0]['volume'])}});
        return ad + (cconts*records[0]['volume']);

# ADN: 近N日的AD值简单平�?
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# duration: 周期，这里取6�?0
# return adman
def caclADMA(code,date,collection,duration,):
    records = collection.find({"date":{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(duration);
    records = list(records);
    if(len(records)<duration or not records[duration-1].has_key('ad') or records[duration-1]['ad'] is None):
        return None;
    else:
        sumAD = 0;
        for item in records:
            if(item['ad'] is not None):
                sumAD += item['ad'];
        return sumAD/duration;
# CoppockCurve: WMA((ROC14+11ROC), 10)
#               ROC11= 100* (当日收盘�?11日前得到收盘�?/11日前的收盘价
#               ROC14= 100*(当日收盘�?14日前的收盘价)/14日前的收盘价
#               COPPOCK CURVE当日=[(10天前的ROC14+ROC11 *) 1 + (9日前ROC14+ ROC11)*2 …�?（当日的roc14+roc11�?10]/55 
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return cc
def caclCoppockCurve(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(10);
    records = list(records);
    
    if(len(records)<10):
        return None;
    else:
        sumROC = 0;
        count = 0;
        while(count<10):
            if(records[count]['roc11'] is not None  and  records[count]['roc14'] is not None):
                sumROC += (records[count]['roc11']+records[count]['roc14'])*(10-count);
            count += 1;
        return sumROC/55;

# SWINGINDEX: 
# 1、A=当天最高价-前一天收盘价
# B= 当天最低价-前一天收盘价
# C= 当天最高价-前一天最低价
# D= 前一天�?�盘�?前一天开盘价
# A、B、C、D 皆采用绝对�?
# 2、E=�?天收盘�?前一天收盘价
# F= 当天收盘�?当天开盘价
# G= 前一天收盘价-前一天开盘价
# E、F、G采用�?－差�?
# 3、X=E+1/2F+G�?
# 4、K=比较A、B两数值，选出其中最大�?
# 5、比较A、B、C三数值：
# 若A最大，则R=A+ 1/2B+ 1/4D
# 若B最大，则R=B+1/2A�?/4D
# 若C最大，则R= C+1/4D
# 6、L=3
# 7、SI= 50* X/R * K/L
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return si        
def caclSI(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(2);
    records = list(records);
    if(len(records)<2 or  records[0]['high'] is None  or  records[0]['low'] is None  or  records[0]['close'] is None  or  records[1]['high'] is None  or  records[1]['low'] is None  or  records[1]['close'] is None):
        collection.update({'_id':records[0]['_id']},{'$set':{'si':None}});
        return None;
    else:
        a = abs(records[0]['high'] - records[1]['close']);
        b = abs(records[0]['low'] - records[1]['close']);
        c = abs(records[0]['high'] - records[1]['low']);
        d = abs(records[1]['close'] - records[1]['open']);
        e = records[0]['close'] - records[1]['close'];
        f = records[0]['close'] - records[0]['open'];
        g = records[1]['close'] - records[1]['open'];
        x = e + float(f)/2 + g;
        k = max(a,b);
        maxabc = max(a,b,c);
        if(maxabc==a):
            r = a + float(b)/2 + float(d)/4;
        elif(maxabc==b):
            r = b + float(a)/2 + float(d)/4;
        elif(maxabc==c):
            r = c + float(d)/4;
        l = 3;
        if(r==0):
            collection.update({'_id':records[0]['_id']},{'$set':{'si':None}});
            return None;
        else:
            collection.update({'_id':records[0]['_id']},{'$set':{'si':50*float(x)/r*k/l}});
            return 50*float(x)/r*k/l;

# Asi: ASI=(si +si昨天)/2
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return asi
def caclASI(code,date,collection):
    # collection = c.history[code];'
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(2);
    records = list(records);
    if(len(records)<2 or  records[0]['si'] is None  or  records[1]['si'] is None):
        return None;
    else:
        return (records[0]['si'] + records[1]['si'])/2;

# ChaikinOscillator:  (3-day EMA of AD)  -  (10-day EMA of AD)             
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return co
def caclCO(code,date,collection):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(10);
    records = list(records);
    if(len(records)<10 or not records[9].has_key('ad') or records[9]['ad'] is None):
        return None;
    else:
        denominator1 = (1+3)*3/2;
        denominator2 = (1+10)*10/2;
        ema3 = 0;
        ema10 = 0;
        count = 0;
        for item in records:
            if(item['ad'] is not None):
                if(count<3):
                    ema3 += (3-count)/denominator1*item['ad'];
                ema10 += (10-count)/denominator2*item['ad'];
        return ema3-ema10;
# HL: EMA ([H-L], 10)            
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return hl
def caclHL(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(10);
    records = list(records);
    if(len(records)<10):
        collection.update({'_id':records[0]['_id']},{'$set':{'hl':None}});
        return None;
    else:
        denominator = (1+10)*10/2;
        count = 0;
        hl = 0;
        while (count<10):
            if(records[count]['high'] is None  or  records[count]['low'] is None):
                hl += 0;
            else:
                hl += float(10-count)*(records[count]['high']-records[count]['low'])/denominator;
            count += 1;
        collection.update({'_id':records[0]['_id']},{'$set':{'hl':hl}});
        return hl;

# ChaikinVolatility: (HL- HL10日前 ) / HL 10日前  * 100           
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return cv
def caclCV(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(10);
    records = list(records);
    record = records[0];
    
    if(len(records)<10  or  record['hl'] is None ):
        return None;
    
    preR = records[9];
    if(preR['hl'] is None  or  preR['hl']==0):
        return None;
    else:
        return (record['hl']-preR['hl'])/preR['hl']*100;

# EMVN: EMV=PR/PV
#       PR=0.5×（H+L）－0.5×(YH+YL)
#       PV=Volume/（H－L�?
#       YH 前日高价 YL前日最低价
#       EMVN=SUM(EMV,N)/N        
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# duration: 周期，这里取6�?4
# return emv
def caclEMV(code,date,collection,duration):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(duration+1);
    records = list(records);
    if(len(records)<duration+1):
        return None;
    else:
        emv = 0;
        count = 0;
        while (count<duration):
            if(records[count]['low'] is None  or  records[count]['high'] is None  or records[count+1]['high'] is None  or records[count+1]['low'] is None ):
                pr = 0;
            else:
                pr = float(records[count]['high']+records[count]['low']-(records[count+1]['high']+records[count+1]['low']))/2;
            if(records[count]['high'] is None  or  records[count]['low'] is None  or  records[count]['high']-records[count]['low']==0  or  records[count]['volume'] is None):
                pv = 0;
            else:
                pv = records[count]['volume']/float(records[count]['high']-records[count]['low']);
            if(pv!=0):
                emv += pr/pv;
            count += 1;
        return emv/duration;

# DI: +DM=H-H1日前，如�?=0�?DM=0
#       -DM=L1日前-L 如果<=0 �?DM=0
#       P比较 +DM�?DM 大的保留，较小的数字�?
#       TR �? |当日最高价-当日最低价|�?|前个交易日收盘加-当日最高价|，|前个交易日收盘加-当日最低价|�?三值中的最大�?
#       PLUSDI =�?DM/TR�?100,   MINUSDI=(-DM/TR)*100   
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return di
def caclDI(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(2);
    records = list(records);
    if(len(records)<2  or  records[0]['high'] is None  or  records[0]['low'] is None  or  records[1]['high'] is None  or  records[1]['low'] is None  or  records[0]['tr'] is None  or  records[0]['tr']==0):
        collection.update({'_id':records[0]['_id']},{'$set':{'plusDI':None,'minusDI':None}});
        # return None,None;
    else:
        dm1 = max(records[0]['high']-records[1]['high'],0);
        dm2 = max(records[1]['low']-records[0]['low'],0);
        if(dm1>dm2):
            dm2 = 0;
        elif(dm2>dm1):
            dm1 = 0;
        if(records[0]['tr']==0):
            collection.update({'_id':records[0]['_id']},{'$set':{'plusDI':None,'minusDI':None}});
        else:
            collection.update({'_id':records[0]['_id']},{'$set':{'plusDI':float(dm1)/records[0]['tr'],'minusDI':float(dm2)/records[0]['tr']}});
        # return float(dm1)/records[0]['tr'],float(dm2)/records[0]['tr']

# ADX: Ma(DX, N) N=12
#       DX=（|plusDI-minusDI|�?(plusDI+minusDI�?100 
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return adx
def caclADX(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(12);
    records = list(records);
    if(len(records)<12 or records[11]['plusDI'] is None or records[11]['minusDI']):
        collection.update({'_id':records[0]['_id']},{'$set':{'adx':None}});
        return None;
    else:
        count = 0;
        adx = 0;
        while(count<12):
            plusDI = records[count]['plusDI'];
            minuxDI = records[count]['minusDI'];
            if(plusDI is None  or  minuxDI is None or plusDI+minuxDI==0):
                adx += 0;
            else:
                adx += abs(plusDI-minuxDI)/float(plusDI+minuxDI)*100;
            count+=1;
        collection.update({'_id':records[0]['_id']},{'$set':{'adx':adx/12}});
        # return adx/12;
# ADXR: ADXR=(ADX+ADX前一�?/2
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return adxr
def caclADXR(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(2);
    records = list(records);
    if(len(records)<2 or records[0]['adx'] is None or records[1]['adx'] is None):
        return None;
    else:
        return float(records[0]['adx']+records[1]['adx'])/2;

# HistoryLowHigh: 历史最高及后几�?
# records: 股票数据
# return low,lowday,high,highday        
def caclHistoryLowHigh(records,collection):
    # collection = c.history[code];
    low = 0;
    high = 0;
    lowday = 0;
    highday = 0;
    if(len(records)<2 or not records[1].has_key('llow')):
        low = records[0]['low'];
        high = records[0]['high'];
        lowday = 0;
        highday = 0;
    else:
        if(records[0]['low']<records[1]['llow']):
            low = records[0]['low'];
            lowday = 0;
            # return records[0]['low'],0;
        else:
            low = records[1]['low'];
            lowday = records[1]['lowday']+1;
            # return records[1]['llow'],records[1]['lowday']+1;
        if(records[0]['high']>records[1]['hhigh']):
            high = records[0]['high'];
            highday = 0;
            # return records[0]['high'],0;
        else:
            high = records[1]['hhigh'];
            highday = records[1]['highday']+1;
            # return records[1]['hhigh'],records[1]['highday']+1;
    collection.update({'_id':records[0]['_id']},{'$set':{'llow':low,'lowday':lowday,'hhigh':high,'highday':highday}});
    return low,lowday,high,highday;

# AroonDown=（M-最低价后的天数�?M*100
# AroonUP=（M-最高价后的天数�?M*100
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# M=25
# return arrondown,arronup
def caclArron(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(2);
    records = list(records);
    if(len(records)<2):
        caclHistoryLowHigh(records,collection);
        return None,None;
    else:
        low,lowday,high,highday = caclHistoryLowHigh(records,collection);
        arrondown = (25-lowday)/25*100;
        arronup =(25-highday)/25*100;
        return arrondown,arronup;

# EMA12 =EMA(收盘价，12)
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return ema12
def caclEMA12(code,date,collection):
    ema = caclCloseEMA(code,date,collection,12);
    return ema;

# EMA12 =EMA(收盘价，26)
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return ema26
def caclEMA26(code,date,collection):
    ema = caclCloseEMA(code,date,collection,26);
    return ema;

# DEA = EMA(DIFF,9)
# DIFF = EMA12-EMA26
# MACD = 2*（DIF-DEA�?
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return dea,macd
def caclDEAMACD(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(9);
    records = list(records);
    if(len(records)<9  or  records[8]['ema26'] is None  or  records[8]['ema12'] is None):
        return None,None;
    else:
        count = 0;
        dea = 0;
        denominator = (1+9)*9/2;
        cdiff = records[0]['ema12'] - records[0]['ema26'];
        while(count<9):
            diff = records[count]['ema12'] - records[count]['ema26'];
            dea += float(9-count)*(diff)/denominator;
            count += 1;
        return dea,2*(cdiff-dea);

# D如果（H+L�?=（H昨日+L昨日），DMZ=0�?
# 如果（H+L�?（H昨日+L昨日），DMZ=|（H-H昨日|与|（L-L昨日）|的绝对值中较大值�?
# 如果（H+L�?=（H昨日+L昨日），DMF=0�?
# 如果（H+L�?（H昨日+L昨日），DMF=|（H-H昨日|与|（L-L昨日）|的绝对值中较大值�?
# DIZ= SUM(DMZ,13)/[(SUM(DMZ,13)+SUM(DMF,13)
# DIF= SUM(DMF,13)/[(SUM(DMZ,13)+SUM(DMF,13)
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return diz,dif        
def caclDIZF(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(14);
    records = list(records);
    if(len(records)<14):
        return None,None;
    else:
        dmz = 0;
        dmf = 0;
        count = 0;
        while(count<13):
            thigh = records[count]['high'];
            tlow = records[count]['low'];
            yhigh = records[count+1]['high'];
            ylow = records[count+1]['low'];
            if(thigh+tlow>yhigh+ylow):
                dmz += max(abs(thigh-yhigh),abs(tlow-ylow));
            elif(thigh+tlow<yhigh+ylow):
                dmf += max(abs(thigh-yhigh),abs(tlow-ylow));
            count += 1;
        if(dmz+dmf==0):
            return None,None;
        return float(dmz)/(dmz+dmf),float(dmf)/(dmz+dmf);

# CN=6日前收盘�?
# MTM(6)=C-CN（这个值不用）
# MTM(6)=（C/CN*100�?100
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return mtm
def caclMTM(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(6);
    records = list(records);
    if(len(records)<6  or  records[5]['close']==0):
        collection.update({'_id':records[0]['_id']},{'$set':{'mtm':None}});
        # return None;
    else:
        mtm1 = records[0]['close'] - records[5]['close'];
        if(records[5]['close']==0):
            collection.update({'_id':records[0]['_id']},{'$set':{'mtm':None}});
        else:
            collection.update({'_id':records[0]['_id']},{'$set':{'mtm':float(records[0]['close']/records[5]['close'])*100-100}});
        # return float(records[0]['close']/records[5]['close'])*100-100;

# Mtmma: MA(MTM,N)
# N=10
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return mtmma
def caclMTMMA(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(10);
    records = list(records);
    if(len(records)<10  or  records[9]['mtm'] is None):
        return None;
    else:
        sumMTM = 0;
        for record in records:
            if(record['mtm'] is not None):
                sumMTM += record['mtm'];
        return sumMTM/10;

# PVT: x�?今日收盘价—昨日收盘价)／昨日收盘价×当日成交�?
#      PVT=SUM(X)从第一个交易日
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return pvt
def caclPVT(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(2);
    records = list(records);
    if(len(records)<2  or  records[0]['volume'] is None):
        collection.update({'_id':records[0]['_id']},{'$set':{'pvt':None}});
        return None;
    else:
        if(records[1]['pvt'] is None):
            if(records[1]['close']==0):
                collection.update({'_id':records[0]['_id']},{'$set':{'pvt':None}});
            else:
                collection.update({'_id':records[0]['_id']},{'$set':{'pvt':float(records[0]['close']-records[1]['close'])/records[1]['close']*records[0]['volume']}});
            # return float(records[0]['close']-records[1]['close'])/records[1]['close']*records[0]['volume'];
        else:
            if(records[1]['close']==0):
                collection.update({'_id':records[0]['_id']},{'$set':{'pvt':None}});
            else:
                collection.update({'_id':records[0]['_id']},{'$set':{'pvt':records[1]['pvt'] + float(records[0]['close']-records[1]['close'])/records[1]['close']*records[0]['volume']}});
            # return records[1]['pvt'] + float(records[0]['close']-records[1]['close'])/records[1]['close']*records[0]['volume'];

# Pvt6: ma（pvt�?�?
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return pvt6
def caclPVT6(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(6);
    records = list(records);
    if(len(records)<6  or  records[5]['pvt'] is None):
        return None;
    else:
        sumPVT = 0;
        for item in records:
            if(item['pvt'] is not None):
                sumPVT += item['pvt'];
        return sumPVT/6;
# Pvt12: ma（pvt�?2�?
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return pvt12
def caclPVT12(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(12);
    records = list(records);
    if(len(records)<12  or  records[11]['pvt'] is None):
        return None;
    else:
        sumPVT = 0;
        for item in records:
            if(item['pvt'] is not None):
                sumPVT += item['pvt'];
        return sumPVT/12;

# EMAEMA12: EMA(EMA(CLOSE,N),N)
# code: 股票代码
# date: 当前日期
# collection: 数据集�??
# N=12
# return emaema12
def caclEMAEMA12(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(12);
    records = list(records);
    if(len(records)<12 or records[11]['ema12'] is None):
        collection.update({'_id':records[0]['_id']},{'$set':{'emaema12':None}});
    else:
        count = 0;
        denominator = (1+12)*12/2;
        sumEMA12 = 0;
        while(count<12):
            sumEMA12 += float((12-count)*records[count]['ema12'])/denominator;
            count += 1;
        collection.update({'_id':records[0]['_id']},{'$set':{'emaema12':sumEMA12}});
# MTR: EMA(EMA(EMA(CLOSE,N),N),N)
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# N=12
# return emaemaema12
def caclEMAEMAEMA12(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(12);
    records = list(records);
    if(len(records)<12 or records[11]['emaema12'] is None):
        collection.update({'_id':records[0]['_id']},{'$set':{'emaemaema12':None}});
        # return None;
    else:
        count = 0;
        denominator = (1+12)*12/2;
        sumEMAEMA12 = 0;
        while(count<12):
            sumEMAEMA12 += float((12-count)*records[count]['emaema12'])/denominator;
            count += 1;
        collection.update({'_id':records[0]['_id']},{'$set':{'emaemaema12':sumEMAEMA12}});
        # return sumEMAEMA12;

# TRIX: (MTR-MTR昨日)/（MTR昨日�?100
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return trix
def caclTRIX(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(2);
    records = list(records);

    if(len(records)<2  or not records[1].has_key('emaemaema12') or  records[1]['emaemaema12'] is None or records[1]['emaemaema12']==0):
        return None;
    else:
        return float(records[0]['emaemaema12']-records[1]['emaemaema12'])/records[1]['emaemaema12']*100;

# TRIXN: MA(TRIX,N)
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# duration: 周期 这里�?�?0
# return trixmaN
def caclTRIXMA(code,date,collection,duration):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(duration);
    records = list(records);
    if(len(records)<duration or not records[duration-1].has_key('trix')  or  records[duration-1]['trix'] is None):
        return None
    else:
        count = 0;
        sumTRIX = 0;
        while(count<duration):
            sumTRIX += records[count]['trix'];
            count += 1;
        return sumTRIX/duration;

# UOS:TH:=MAX(HIGH,REF(CLOSE,1));
#     TL:=MIN(LOW,REF(CLOSE,1));
#     ACC1:=SUM(CLOSE-TL,N1)/SUM(TH-TL,N1);
#     ACC2:=SUM(CLOSE-TL,N2)/SUM(TH-TL,N2);
#     ACC3:=SUM(CLOSE-TL,N3)/SUM(TH-TL,N3);
#     UOS:(ACC1*N2*N3+ACC2*N1*N3+ACC3*N1*N2)*100/(N1*N2+N1*N3+N2*N3);
#     n1=7,n2-14,n3=28
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return uos
def caclUOS(code,date,collection):
    n1 = 7;
    n2 = 14;
    n3 = 28;
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(n3+1);
    records = list(records);
    totalcount = len(records);
    if(totalcount<n3+1 ):
        return None;
    else:
        ct1 = 0;
        tt1 = 0;
        ct2 = 0;
        tt2 = 0;
        ct3 = 0;
        tt3 = 0;
        acc1 = 0;
        acc2 = 0;
        acc3 = 0;
        count = 0;
        while(count<totalcount-1):
            th = max(records[count]['high'],records[count+1]['close']);
            tl = min(records[count]['low'],records[count+1]['close']);
            if(count<n1):
                ct1 += records[count]['close']-tl;
                tt1 += th - tl;
            if(count<n2):
                ct2 = records[count]['close'] - tl;
                tt2 = th - tl;
            ct3 = records[count]['close'] - tl;
            tt3 = th - tl;
            count += 1;
        if(tt1>0):
            acc1 = float(ct1)/tt1;
        if(tt2>0):
            acc2 = float(ct2)/tt2;
        if(tt3>0):
            acc3 = float(ct3)/tt3;
        return float(acc1*n2*n3+acc2*n1*n3+acc3*n1*n2)*100/(n1*n2+n1*n3+n2*n3);

# MA10RegressCoeff6: 10日价格平均线6日线性回归系�?
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return mrc6
def caclMA10RegressCoeff6(code,date,collection):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(6);
    records = list(records);
    if(len(records)<6):
        return None;
    x = [1,2,3,4,5,6];
    y =  [float(z['ma10']) for z in records];
    a,b = np.polyfit(x, y, 1);
    return a;

# MA10RegressCoeff12: 10日价格平均线12日线性回归系�?
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return mrc12
def caclMA10RegressCoeff12(code,date,collection):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(12);
    records = list(records);
    if(len(records)<12):
        return None;
    x = [1,2,3,4,5,6,7,8,9,10,11,12];
    y = [z['ma10'] for z in records];
    a,b = np.polyfit(x, y, 1);
    return a;

# PLRC6: 6日收盘价格线性回归系�?
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return plrc6
def caclPLRC6(code,date,collection):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(6);
    records = list(records);
    if(len(records)<6):
        return None;
    x = [1,2,3,4,5,6];
    y = [z['close'] for z in records];
    a,b = np.polyfit(x, y, 1);
    return a;

# PLRC12: 12日收盘价格线性回归系�?
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return plrc12
def caclPLRC12(code,date,collection):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(12);
    records = list(records);
    if(len(records)<12):
        return None;
    x = [1,2,3,4,5,6,7,8,9,10,11,12];
    y = [z['close'] for z in records];
    a,b = np.polyfit(x, y, 1);
    return a;

# RI: 100*（C-10日内最高）/10日内最�?
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# duration: 周期 �?0�?
# return ri
def caclRI(code,date,collection,duration):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(duration);
    records = list(records);
    if(len(records)<duration):
        return None;
    else:
        llh = records[0]['high'];
        for item in records:
            if(item['high']>llh):
                llh = item['high'];
        if(llh==0):
            return None;
        else:
            return 100*(records[0]['close']-llh)/llh;

# Hurst: 暂无公式
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return hurst 
def caclHurst(code,date,collection):
    # TODO
    return;

# APBMA: 暂无公式
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return apbma 
def caclAPBMA(code,date,collection):
    # TODO
    return;

# BBI: (MA(CLOSE,M1)+MA(CLOSE,M2)+MA(CLOSE,M3)+MA(CLOSE,M4))/4;
# BBIC: BBI/CLOSE
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return bbi,bbic
def caclBB(code,date,collection):
    m1 = 3;
    m2 = 6;
    m3 = 12;
    m4 = 24;
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(m4); 
    records = list(records);
    if(len(records)<m4):
        return None,None;
    else:
        c1 = 0;
        c2 = 0;
        c3 = 0;
        c4 = 0;
        count = 0;
        for item in records:
            if(count<m1):
                c1 += item['close'];
            if(count<m2):
                c2 += item['close'];
            if(count<m3):
                c3 += item['close'];
            c4 += item['close'];
            count += 1;
        ac1 = float(c1)/m1;
        ac2 = float(c2)/m2;
        ac3 = float(c3)/m3;
        ac4 = float(c1)/m4;
        bbi = float(ac1+ac2+ac3+ac4)/4;
        if(records[0]['close']==0  or  records[0]['close'] is None):
            bbic = None;
        else:
            bbic = bbi/records[0]['close'];
        return bbi,bbic;

# TEMAN: EMA(TRIX,5) 
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# duration: 周期 5,10
# return tema
def caclTEMA(code,date,collection,duration):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(duration);
    records = list(records);
    if(len(records)<duration or not records[duration-1].has_key('trix')  or  records[duration-1]['trix'] is None):
        return None;
    else:
        denominator = (1+duration)*duration/2;
        sumT = 0;
        count = duration;
        for item in records:
            if(item['trix'] is not None and item['trix']!=0):
                sumT += float(count)/denominator*item['trix'];
            count -= 1;
        return sumT;

# AR: N日内（当日最高价—当日开市价）之�?/ N日内（当日开市价—当日最低价）之�?
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# N=26
# return ar
def caclAR(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(26);
    records = list(records);
    if(len(records)<26):
        collection.update({'_id':records[0]['_id']},{'$set':{'ar':None}});
        # return None;
    else:
        ho = 0;
        ol = 0;
        for item in records:
            ho += (item['high']-item['open']);
            ol += (item['open']-item['low']);
        if(ol==0):
            collection.update({'_id':records[0]['_id']},{'$set':{'ar':None}});
            # return None;
        else:
            collection.update({'_id':records[0]['_id']},{'$set':{'ar':float(ho)/ol}});
            # return float(ho)/ol;

# BR: N日内（当日最高价—前一日收市价）之�?/ N日内（前一日收市价—当日最低价）之�?
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# N=26
# return br
def caclBR(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(27);
    records = list(records);
    if(len(records)<27):
        collection.update({'_id':records[0]['_id']},{'$set':{'br':None}});
        # return None;
    else:
        hc = 0;
        cl = 0;
        count = 0;
        while(count<26):
            hc += (records[count]['high']-records[count+1]['close']);
            cl += (records[count+1]['close']-records[count]['low']);
            count += 1;
        if(cl==0):
            collection.update({'_id':records[0]['_id']},{'$set':{'br':None}});
            # return None;
        else:
            collection.update({'_id':records[0]['_id']},{'$set':{'br':float(hc)/cl}});
            # return float(hc)/cl;

# ARBR: AR-BR
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return arbr
def caclARBR(code,date,collection):
    # collection = c.history[code];
    record = collection.find_one({'date':date});
    if(not record or record['ar'] is None  or  record['br'] is None):
        return None;
    else:
        return record['ar'] - record['br'];

# CR20: MID:=REF(HIGH+LOW,1)/2;
#       CR:SUM(MAX(0,HIGH-MID),N)/SUM(MAX(0,MID-LOW),N)*100;
#       N=20  
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return cr20
def caclCR20(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(21);
    records = list(records);
    if(len(records)<21):
        return None;
    else:
        count = 0;
        hm = 0;
        ml = 0;
        while(count<20):
            mid = float(records[count+1]['high'] + records[count+1]['low'])/2;
            hm += max(records[count]['high']-mid,0);
            ml += max(0,mid-records[count]['low']);
            count += 1;
        if(ml<=0):
            return None;
        else:
            return float(hm)/ml*100;

# HmLMA: 获取N天内H-L的均值，H-L均值的均�?
# records: 股票数据
# return tohmlma,tohmlmama
def getHmLMA(records,collection):
    sumMa = 0;
    sumMAMA = 0;
    for item in records:
        sumMa += (item['high'] - item['low']);
        if(item.has_key('hmlma') and item['hmlma'] is not None):
            sumMAMA += item['hmlma'];
    tohmlma = sumMa/len(records);   
    collection.update({'_id':records[0]['_id']},{'$set':{'hmlma':tohmlma,'hmlmama':float(tohmlma+sumMAMA)/len(records)}});
    return tohmlma,float(tohmlma+sumMAMA)/len(records);

# MassIndex: SUM(MA(HIGH-LOW,N1)/MA(MA(HIGH-LOW,N1),N1),N2)
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# N1=9,N2=25
# return mi
def caclMassIndex(code,date,collection):
    n1 = 9;
    n2 = 25;
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(n2);
    records = list(records);
    if(len(records)>n1):
        tohmlma,tohmlmama = getHmLMA(records[0:n1-1],collection);
    if(len(records)<n2 or not records[24].has_key('hmlma') or not records[24].has_key('hmlmama')):
        return None;
    
    records[0]['hmlma'] = tohmlma;
    records[0]['hmlmama'] = tohmlmama;
    
    flag = True;
    sumDvi = 0;
    for item in records:
        if(item['hmlmama'] == 0):
            sumDvi += 0;
        else:
            sumDvi += item['hmlma']/item['hmlmama'];
    return sumDvi;

# Bearpower: 日内最低价-EMA（收盘价�?3�?
# Bullpower: 日�?�最高价-EMA（收盘价�?3�?
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return bearpower,bullpower
def caclBearBullPower(code,date,collection):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(13);
    records = list(records);
    if(len(records)<13):
        return None,None;
    ema13 = caclCloseEMA(code,date,collection,13);
    return (records[0]['low']-ema13),(records[0]['high']-ema13);

# 股价向后复权�? 除权�?（除权前一日收盘价+配股�?配股比率－每股派息）/�?+配股比率+送股比率�?
#                 单次除权因子=收盘�?除权�?   （登记日�?
#                 复权因子=上市以来所有除权因子的乘积
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return backwardadj
def caclBackwardADJ(code,date,collection):
    #TODO
    return;

# 股价偏度: 
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# duration: 周期，这里取5�?0�?0�?0�?20�?40
# return skewness
def caclSkewness(code,date,collection,duration):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(duration);
    records = list(records);
    if(len(records)<duration):
        return None;
    else:
        sumClose = 0;
        for item in records:
            sumClose += item['close'];
        avaClose = records[0]['close']/duration;
        sumSD = 0;
        sumDelta = 0;
        for item in records:
            delta = item['close'] - avaClose;
            sumDelta += float(delta)*delta*delta;
            sumSD = float(delta)*delta;
        sd = math.sqrt(sumSD/duration);
        if(sd>0):
            return float(sumDelta)/((duration-1)*(duration-2)*math.pow(sd,3));
        else:
            return 0;

# 换手率相对波动率: 过去n日换手率的标准差，n=5�?0�?0�?0�?20�?40（可以下面的均值结合统计）
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# duration: 周期
# return t_man,volsdn
def caclVolatility(code,date,collection,duration):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(duration);
    records = list(records);
    if(len(records)<duration):
        collection.update({'_id':records[0]['_id']},{'$set':{'tma'+str(duration):None}});
        return None,None;
    else:
        sumTurnover = 0;
        for item in records:
            if(item['turnover'] is not None):
                sumTurnover += item['turnover'];
        avaTurnove = sumTurnover/duration;
        sumSD = 0;
        for item in records:
            if(item['turnover'] is not None):
                delta = item['turnover'] - avaTurnove;
                sumSD = float(delta)*delta;
        sd = math.sqrt(sumSD/duration);
        collection.update({'_id':records[0]['_id']},{'$set':{'tma'+str(duration):avaTurnove}});
        return avaTurnove,sd;

# 52周高�? 过去一年小于当日收盘价个数/过去一年收盘价总个�?
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return ftw
def caclFTW(code,date,collection):
    # collection = c.history[code];
    end = date;
    ss = date.split('-');
    year = int(ss[0]);
    pyear = int(year)-1;
    begin = str(pyear)+'-'+ss[1]+'-'+ss[2];
    record = collection.find_one({'date':date});
    count1 = collection.find({'date':{'$gte':begin,'$lte':end},'close':{'$lt':record['close']}}).count();
    count2 = collection.find({'date':{'$gte':begin,'$lte':end}}).count();
    if(count2<1):
        return 0;
    else:
        return float(count1)/count2;

# 8季度净利润变化趋势: �?个季度的净利润，如果同比（去年同期）增长记�?1，同比下滑记�?1，再�? 个值相加�?
# code: 股票代码
# date: 当前日期
# return earnmom
def caclEARNMOM(code,date):
    end = date;
    ss = date.split('-');
    year = int(ss[0]);
    month = int(ss[1]);
    if(month<4):
        quarter = 1;
    elif(month<7):
        quarter = 2;
    elif(month<10):
        quarter = 3;
    elif(month<13):
        quarter = 4;
    pers = c.quarter[code].find({'year':{'$lte':year}}).sort([("year",mongo.DESCENDING),("quarter",mongo.DESCENDING)]).limit(8);
    pers = list(pers);
    if(len(pers)<8):
        return None;
    else:
        sumComp = 0;
        for item in pers:
            if(item['profits_yoy']>0):
                sumComp += 1;
            elif(item['profits_yoy']<0):
                sumComp -= 1;
        return sumComp;

# 未预期毛�? 暂无公式
# code: 股票代码
# date: 当前日期
# collection: 数据集合
# return earnmom
def caclSUQI(code,date):
    # TODO
    return;

# 毛利率增�? （今年毛利率/去年毛利率）-1
# code: 股票代码
# date: 当前日期
# return degm
def caclDEGM(code,date):
    end = date;
    ss = date.split('-');
    year = int(ss[0]);
    month = int(ss[1]);
    if(month<4):
        quarter = 1;
    elif(month<7):
        quarter = 2;
    elif(month<10):
        quarter = 3;
    elif(month<13):
        quarter = 4;
    pers = c.quarter[code].find({'year':{'$lte':year},'quarter':quarter}).limit(2);
    pers = list(pers);
    if(len(pers)<2):
        return None;
    else:
        if(pers[1]['gross_profit_rate']==0):
            return None;
        return pers[0]['gross_profit_rate']/pers[1]['gross_profit_rate']-1;

# 现金流资产比与资产回报率之差: （现金流净�?净利润�?企业总资�?
# code: 股票代码
# date: 当前日期
# record: 本季度基本面数据
# return acca
def caclACCA(code,date,record):

    return (record['epcf']*record['totals']*100000000-record['net_profits']*10000)/(record['totalAssets']*10000);

# CFO2EV: 现金流量净�?企业价�?
# code: 股票代码
# date: 当前日期
# record: 本季度基本面数据
# close: 本季度最后一天的股价
# return cfo2ev
def caclCFO2EV(code,date,record,close):
    marketValue = close * record['totals']*100000000;

    return (record['epcf']*record['totals']*100000000)/(marketValue);

# TA2EV: 企业总资�?企业价�?
# code: 股票代码
# date: 当前日期
# record: 本季度基本面数据
# close: 本季度最后一天的股价
# return ta2ev
def caclTA2EV(code,date,record,close):
    marketValue = close * record['totals']*100000000;
    return record['totalAssets']*10000/marketValue;

# 威廉变异离散�? A=当天收盘价－当天开盘价，B=当天最高价－当天最低价
#                 V=当天成交金额，WVAD=�?A÷B×V) n�?天与12�?
# code: 股票代码
# date: 当前日期
# collection: 股票数据
# duration: 周期，这里取6�?2
# return wvad
def caclWVAD(code,date,collection,duration):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(duration);
    records = list(records);
    if(len(records)<duration or not records[duration-1].has_key('amount') or records[duration-1]['amount'] is None):
        return None;
    else:
        sumAd = 0;
        for item in records:
            a = item['close']-item['open'];
            b = item['high'] - item['low'];
            if(b<=0 or not item.has_key('amount') or item['amount'] is None):
                sumAd+=0;
            else:
                sumAd += float(a)/b*float(item['amount']);
        return sumAd;

# 总利润增长率: （今年净利润/去年净利润�?1
# code: 股票代码
# date: 当前日期
# return tpgr
def caclTPGR(code,date):
    ss = date.split('-');
    year = int(ss[0]);
    month = int(ss[1])-1;
    if(month<4):
        quarter = 1;
    elif(month<7):
        quarter = 2;
    elif(month<10):
        quarter = 3;
    elif(month<13):
        quarter = 4;
    pers = c.quarter[code].find({'year':{'$lte':year},'quarter':quarter}).sort([("year",mongo.DESCENDING)]).limit(2);
    pers = list(pers);
    if(len(pers)<2):
        return None;
    else:
        return pers[0]['net_profits']/pers[1]['net_profits']-1;

# 总利润成本比�? 利润总额/(营业成本+财务费用+销售费�?管理费用)
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# return tpcr
def caclTPCR(code,date,record):
    # TODO
    return;

# 总资产周转率: 营业收入/总资�?
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# return tatr
def caclTATR(code,date,record):

    if(record is None or record['totalAssets']<=0):
        return None;
    return record['business_income']*1000000/(record['totalAssets']*10000);

# 总资产增长率: （今年总资产d/去年总资产）-1
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# return tagr
def caclTAGR(code,date,record):
    return record['targ'];

# 超额流动: 暂无公式
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# return tobt
def caclTOBT(code,date):
    # TODO
    return;

# 销售税金率: 营业税金及附�?营业收入
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# return taxratio
# 暂无数据�?
def caclTaxRatio(code,date):
    # TODO
    return;

# 未预期盈�? (最近一年净利润-除去最近一年的过往净利润均�?/ 除去最近一年的过往净利润标准�?
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# return sue
def caclSUE(code,date):
    ss = date.split('-');
    year = int(ss[0]);
    month = int(ss[1])-1;
    if(month<4):
        quarter = 1;
    elif(month<7):
        quarter = 2;
    elif(month<10):
        quarter = 3;
    elif(month<13):
        quarter = 4;
    pers = c.quarter[code].find({'year':{'$lte':year},'quarter':quarter}).sort([("year",mongo.DESCENDING)]);
    pers = list(pers);
    if(len(pers)<2):
        return None;
    count = 1;
    total = len(pers);
    stat = np.array([x['net_profits']*10000 for x in pers]);
    ava = np.mean(stat);
    std = np.std(stat);
    if(std<=0):
        return None;
    return (pers[0]['net_profits']*10000-ava)/std;

# Sale Service Cash To OR: 销售商品和提供劳务收到的现�?营业收入
# code: 股票代码
# date: 当前日期
# return sscto
# 暂无数据�?
def caclSSCTO(code,date):
    # TODO
    return;

# 销售成本率: 营业成本/营业收入
# code: 股票代码
# date: 当前日期
# return scr
# 暂无数据�?
def caclSCR(code,date):
    # TODO
    return;

# n月相对强�? 暂无公式
# code: 股票代码
# date: 当前日期
# return rstr
def caclRSTR(code,date):
    # TODO
    return;

# 相对强弱指标: n日RSI =n日内收盘涨幅的均�?(n日内收盘涨幅均�?n日内收盘跌幅均�?   收盘涨幅指当日收盘价与前一日相比涨的幅度（若跌不计入、跌幅类似）
# code: 股票代码
# date: 当前日期
# collection: 股票数据集合
# duration: 周期 这里�?�?2�?4
# return rsi
def caclRSI(code,date,collection,duration):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(duration);
    records = list(records);
    if(len(records)<duration):
        return None;
    else:
        sumup = 0;
        sumdown = 0;
        sumava = 0;
        up = 0;
        down = 0;
        aup = 0;
        adown = 0;
        for item in records:
            if(item['p_change']>0):
                sumup += item['p_change'];
                up += 1;
            elif(item['p_change']<0):
                sumdown += item['p_change'];
                down += 1;
            sumava += item['p_change'];
        if(up>0):
            aup = float(sumup)/up;
        if(down>0):
            adown = float(sumdown)/down;
        # ava = float(sumava)/duration;
        if(aup+adown==0):
            return None;
        return aup/(aup+adown);

# 权益回报�? n年净利润/股东权益    n=1�?
# code: 股票代码
# date: 当前日期
# duration: 周期，这里取1�?
# return roen
def caclROE(code,date,duration):
    ss = date.split('-');
    year = int(ss[0]);
    year5 = year-(duration-1);
    month = int(ss[1])-1;
    if(month<4):
        quarter = 1;
    elif(month<7):
        quarter = 2;
    elif(month<10):
        quarter = 3;
    elif(month<13):
        quarter = 4;
    pers = c.quarter[code].find({'year':{'$lte':year,'$gte':year5},'quarter':quarter}).sort([("year",mongo.DESCENDING)]);
    pers = list(pers);
    stat = np.array([x['net_profits']*10000 for x in pers]);
    sumPro = np.sum(stat);
    holdRight = pers[0]['bvps']*pers[0]['totals']*100000000;
    if(holdRight<=0):
        return None;
    return float(sumPro)/holdRight;

# 资本回报�? n年净利润/总资�?     n=1�?
# code: 股票代码
# date: 当前日期
# duration: 周期，这里取1�?
# return roan    
def caclROA(code,date,duration):
    ss = date.split('-');
    year = int(ss[0]);
    year5 = year-(duration-1);
    month = int(ss[1])-1;
    if(month<4):
        quarter = 1;
    elif(month<7):
        quarter = 2;
    elif(month<10):
        quarter = 3;
    elif(month<13):
        quarter = 4;
    pers = c.quarter[code].find({'year':{'$lte':year,'$gte':year5},'quarter':quarter}).sort([("year",mongo.DESCENDING)]);
    pers = list(pers);
    stat = np.array([x['net_profits']*10000 for x in pers]);
    sumPro = np.sum(stat);
    totalAssets = pers[0]['totalAssets']*10000;
    if(totalAssets<=0):
        return None;
    return float(sumPro)/totalAssets;

# REVSn: n日收�?当日收盘�?n日前收盘�?   n=5�?0�?0
# code: 股票代码
# date: 当前日期
# collection: 股票数据集合
# duration: 周期 这里�?�?0�?0
# return revsn    
def caclREVS(code,date,collection,duration):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(duration);
    records = list(records);
    if(len(records)<duration or records[duration-1]==0):
        return None;
    return float(records[0]['close'])/records[duration-1]['close'];

# 速动比率: 流动资产合计-存货)/ 流动负债合�?
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# return quickratio    
def caclQR(code,date,record):
    return record['quickratio'];

# 心理线指�? n日内上涨天数/n     n=12�?
# code: 股票代码
# date: 当前日期
# collection: 股票数据集合
# duration: 周期 这里�?,12
# return psy 
def caclPSY(code,date,collection,duration):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(duration);
    records = list(records);
    if(len(records)<duration):
        return None;
    else:
        # count = collection.find({'date':{'$lte':date},{''}}).sort([("date",mongo.DESCENDING)]).limit(duration);
        count = 0;
        for item in records:
            if(item['p_change']>0):
                count += 1;
        return count/duration;

# CFO2EV: 现金流量净�?企业价�?
# code: 股票代码
# date: 当前日期
# record: 本季度基本面数据
# close: 本季度最后一天的股价
# return ps
def caclPS(code,date,record,close):
    marketValue = close * record['totals']*100000000;
    
    return marketValue/record['business_income']*1000000;

# 市盈�? 总市�?净利润
# code: 股票代码
# date: 当前日期
# record: 本季度基本面数据
# return pe
def caclPE(code,date,record):
    return record['pe'];

# 市现�? 总市�?现金流量净�?
# code: 股票代码
# date: 当前日期
# record: 本季度基本面数据
# close: 本季度最后一天的股价
# return pcf
def caclPCF(code,date,record,close):
    marketValue = close * record['totals']*100000000;
    flows = record['epcf']*record['totals']*100000000;
    if(flows<=0):
        return None;
    return float(marketValue)/flows

# 市净�? 总市�?所有者权�?
# code: 股票代码
# date: 当前日期
# record: 本季度基本面数据
# return pb
def caclPB(code,date,record):
    return record['pb'];

# 现金流量负债比: 现金流量近额/流动负债合�?
# code: 股票代码
# date: 当前日期
# record: 本季度基本面数据
# return cf_liabilities
def caclOCITCL(code,date,record):
    return pers['cf_liabilities'];

# Oper Cash Grow Rate: 今年现金流量/去年现金流量-1
# code: 股票代码
# date: 当前日期
# return ocgr
def caclOCGR(code,date):
    ss = date.split('-');
    year = int(ss[0]);
    year1 = year-1;
    month = int(ss[1])-1;
    if(month<4):
        quarter = 1;
    elif(month<7):
        quarter = 2;
    elif(month<10):
        quarter = 3;
    elif(month<13):
        quarter = 4;
    persy = c.quarter[code].find_one({'year':year,'quarter':quarter});

    persy1 = c.quarter[code].find_one({'year':year1,'quarter':quarter});

    if(persy1 is None):
        return None;
    ty = persy['epcf'] * persy['totals'];
    yy = persy1['epcf'] * persy1['totals'];
    return ty/yy-1;

# 营业收入增长�? 今年营业收入/去年营业收入-1
# code: 股票代码
# date: 当前日期
# return orgr
def caclORGR(code,date):
    ss = date.split('-');
    year = int(ss[0]);
    year1 = year-1;
    month = int(ss[1])-1;
    if(month<4):
        quarter = 1;
    elif(month<7):
        quarter = 2;
    elif(month<10):
        quarter = 3;
    elif(month<13):
        quarter = 4;
    persy = c.quarter[code].find_one({'year':year,'quarter':quarter});

    persy1 = c.quarter[code].find_one({'year':year1,'quarter':quarter});

    if(persy1 is None):
        return None;
    return persy['business_income']/persy1['business_income']-1;

# Operating Profit To TOR: 营业利润/营业总收�?
# code: 股票代码
# date: 当前日期
# return optt
# 暂无数据�?
def caclOPTT(code,date):
    # TODO
    return;

# 营业利润�? 营业利润/营业收入
# code: 股票代码
# date: 当前日期
# return opr
# 暂无数据�?
def caclOPR(code,date):
    # TODO
    return;

# 营业利润增长�? （今年营业利�?去年营业利润�?1
# code: 股票代码
# date: 当前日期
# return opgr
# 暂无数据�?
def caclOPGR(code,date):
    # TODO
    return;

# Operating Expense Rate: 销售费�?营业总收�?
# code: 股票代码
# date: 当前日期
# return oer
# 暂无数据�?
def caclOER(code,date):
    # TODO
    return;

# NP To TOR: 净利润/营业收入 
# code: 股票代码
# date: 当前日期
# record:当前面数�?
# return nptotor
def caclNPtoTOR(code,date,record):
    return record['net_profits']/record['business_income'];

# NP Parent Company Grow Rate: （今年净利润/去年净利润�?1
# code: 股票代码
# date: 当前日期
# record:当前面数�?
# return nppcgr   
def caclNPPCGR(code,date,record):
    
    return record['profits_yoy'];

# 非流动资产比�? 非流动资�?总资�?
# code: 股票代码
# date: 当前日期
# record:当前面数�?
# return ncar 
def caclNCAR(code,date,record):
    return float(record['fixedAssets'])/(record['totalAssets']*10000);

# NOCF To Operating NI: 现金流量净�?(营业收入*毛利�?
# code: 股票代码
# date: 当前日期
# return nocftooni
# 暂无数据�?   
def caclNOCFtoONI(code,date):
    # TODO
    return;

# 销售净利率: 净利润/营业收入
# code: 股票代码
# date: 当前日期
# record:当前面数�?
# return npr     
def caclNPR(code,date,record):
    return float(record['bvps']*record['totals']*100000000)/record['business_income'];

# 净利润增长�? （今年净利润/去年净利润�?1
# code: 股票代码
# date: 当前日期
# record:当前面数�?
# return nprg 
def caclNPGR(code,date,record):
    return pers['nprg'];

# 净资产增长�? （今年股东权�?去年股东权益�?1
# code: 股票代码
# date: 当前日期
# record:当前面数�?
# return nagr 
def caclNAGR(code,date,record):
    return pers['nav'];

# 市场杠杆: 负�?（负�?总市值）
# code: 股票代码
# date: 当前日期
# record:当前面数�?
# close: 本季度最后一天的股价
# return mlev 
def caclMLEV(code,date,record,close):
    debt = record['totalAssets']*10000-record['bvps']*record['totals']*100000000;
    marketValue = close * record['totals']*100000000;
    return float(debt)/(debt+marketValue);

# MAWVAD 6: WVAD6�?日均�?
# code: 股票代码
# date: 当前日期
# collection:股票数据集合
# return wvadma6 
def caclWVADMA6(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(6);
    records = list(records);
    if(len(records)<6 or records[5]['wvad6'] is None):
        return None;
    else:
        # count = collection.find({'date':{'$lte':date},{''}}).sort([("date",mongo.DESCENDING)]).limit(duration);
        sumWVAD = 0;
        for item in records:
            sumWVAD += item['wvad6'];
        return sumWVAD/6;
# MAWVAD 12: WVAD12�?日均�?
# code: 股票代码
# date: 当前日期
# collection:股票数据集合
# return wvadma12 
def caclWVADMA12(code,date,collection):
    # collection = c.history[code];
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(6);
    records = list(records);
    if(len(records)<6 or records[5]['wvad12'] is None):
        return None;
    else:
        # count = collection.find({'date':{'$lte':date},{''}}).sort([("date",mongo.DESCENDING)]).limit(duration);
        sumWVAD = 0;
        for item in records:
            sumWVAD += item['wvad12'];
        return sumWVAD/6;
# 负债资产比: 负�?总资�?
# code: 股票代码
# date: 当前日期
# record:当前面数�?
# close: 本季度最后一天的股价
# return tdta 
def caclTDTA(code,date,record,close):
    debt = record['totalAssets']*10000-record['bvps']*record['totals']*100000000;
    return float(debt)/(record['totalAssets']*10000);

# 长期负债营运资金比: 非流动负债�?��?(流动资产合计-流动负债合�?
# code: 股票代码
# date: 当前日期
# return ldtwc
# 暂无数据�?
def caclLDTWC(code,date):
    # TODO
    return;

# 长期借款资产�? 长期借款/总资�?
# code: 股票代码
# date: 当前日期
# return ldta
# 暂无数据�?
def caclLDTA(code,date):
    # TODO
    return;
# 对数流通市�? 流通市值的对数
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# close: 本季度最后一天的股价
# return lflo
def caclLFLO(code,date,record,close):
    flows = close*record['outstanding']*100000000;
    return math.log(flows);

# 对数市�? 流通市值的对数
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# close: 本季度最后一天的股价
# return lcap
def caclLCAP(code,date,record,close):
    marketValue = close*record['totals']*100000000;
    return math.log(marketValue);

# 投资现金增长�? (今年现金流量净�?去年现金流量净�?-1
# code: 股票代码
# date: 当前日期
# return lcgr
# 暂无数据�?
def caclICGR(code,date):
    # TODO
    return;

# 存货周转�? 营业成本/存货
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# return inventory_turnover   
def caclITR(code,date,record):
    return record['inventory_turnover'];

# 存货周转天数: 360/存货周转�?
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# return inventory_days  
def caclITD(code,date,record):
    return record['inventory_days'];

# 无形资产比率: (无形资产+研发支出+商誉)/总资�?
# code: 股票代码
# date: 当前日期
# return iar
# 暂无数据�?
def caclIAR(code,date):
    # TODO
    return;

# 历史波动: 暂无公式
# code: 股票代码
# date: 当前日期
# return hsigma
def caclHSIGMA(code,date):
    # TODO
    return;

# 历史贝塔: 暂无公式
# code: 股票代码
# date: 当前日期
# return hbeta
def caclHBETA(code,date):
    # TODO
    return;

# 毛利�? 
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# return gpr  
def caclGIR(code,date,record):
    return record['gpr'];

# 流动资产周转�? 
# code: 股票代码
# date: 当前日期
# return currentasset_turnover
def caclFATR(code,date,record):
    return record['currentasset_turnover'];

# 固定资产比率: (固定资产)/总资�?
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# return far     
def caclFAR(code,date,record):
    return float(record['fixedAssets'])/float(record['totalAssets']*10000);

# 流动资产比率: (流动资产)/总资�?
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# return lar     
def caclLAR(code,date,record):
    return float(record['liquidAssets'])/float(record['totalAssets']*10000);

# 筹资活动现金流增长率: (今年筹资活动产生的现金流量净�?去年筹资活动产生的现金流量净�?-1
# code: 股票代码
# date: 当前日期
# return fcgr
# 暂无数据�?
def caclFCGR(code,date):
    # TODO
    return;

# Financial Expense Rate: 财务费用/营业总收�?
# code: 股票代码
# date: 当前日期
# return fer
# 暂无数据�?
def caclFER(code,date):
    # TODO
    return;

# 收益市值比: 净利润/总市�?
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# close: 本季度最后一天的股价
# return etp    
def caclETP(code,date,record,close):
    marketValue = close*record['totals']*100000000;
    return float(record['net_profits']*10000)/marketValue

# 股东权益周转�? 营业收入/股东权益
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# return etr     
def caclETR(code,date,record):
    bvps = record['bvps'] * record['totals'] * 100000000;
    return float(record['business_income']*1000000)/bvps;

# 股东权益比率: 股东权益/总资�?
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# return sheqratio   
def caclETA(code,date,record):
    return record['sheqratio'];

# 股东固定资产比率: 股东权益/(固定资产)
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# return efar   
def caclEFAR(code,date,record):
    return (record['sheqratio'] * record['totalAssets']*10000)/record['fixedAssets'];

# 每股收益: 当年净利润/股票�?
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# return eps   
def caclEPS(code,date,record):
    return record['eps'];

# 5年收益增长率: 5年收益关于时间（年）进行线性回归的回归系数/5 年收益均值的绝对�?
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# return egro    
def caclEGRO(code,date):
    ss = date.split('-');
    year = int(ss[0]);
    month = int(ss[1])-1;
    if(month<4):
        quarter = 1;
    elif(month<7):
        quarter = 2;
    elif(month<10):
        quarter = 3;
    elif(month<13):
        quarter = 4;
    pers1 = c.quarter[code].find({'year':year,'quarter':{'$lte':quarter}});
    pers1 = list(pers1);
    if(len(pers1)<1):
        return None;
    pers2 = c.quarter[code].find({'year':year-1,'quarter':{'$lte':quarter}});
    pers2 = list(pers2);
    if(len(pers2)<1):
        return None;
    pers3 = c.quarter[code].find({'year':year-2,'quarter':{'$lte':quarter}});
    pers3 = list(pers3);
    if(len(pers3)<1):
        return None;
    pers4 = c.quarter[code].find({'year':year-3,'quarter':{'$lte':quarter}});
    pers4 = list(pers4);
    if(len(pers4)<1):
        return None;
    pers5 = c.quarter[code].find({'year':year-4,'quarter':{'$lte':quarter}});
    pers5 = list(pers5);
    if(len(pers5)<1):
        return None;
    benifit1 = 0;
    benifit2 = 0;
    benifit3 = 0;
    benifit4 = 0;
    benifit5 = 0;
    for t1 in pers1:
        benifit1 += t1['esp'] * t1['totals'] * 100000000;
    for t2 in pers2:
        benifit2 += t2['esp'] * t2['totals'] * 100000000;
    for t3 in pers3:
        benifit3 += t3['esp'] * t3['totals'] * 100000000;
    for t4 in pers4:
        benifit4 += t4['esp'] * t4['totals'] * 100000000;
    for t5 in pers5:
        benifit5 += t5['esp'] * t5['totals'] * 100000000;
    x = [1,2,3,4,5];
    y = [benifit1,benifit2,benifit3,benifit4,benifit5];
    a,b = np.polyfit(x, y, 1);
    mean = abs(np.mean(y));
    if(mean<=0):
        return None;
    return a/mean;

# 息税前利润与营业总收入之�? (利润总额+利息支出-利息收入)/ 营业总收�?
# code: 股票代码
# date: 当前日期
# 暂无数据�?
def caclEBITtoTOR(code,date):
    # TODO
    return;

# 收益相对波动: n日里日收益率的标准差   n=5�?0�?0�?0�?20
#               日收�?(今天收盘-昨日收盘)/昨日收盘
# code: 股票代码
# date: 当前日期
# collection: 股票数据集合
# duration: 周期
# return dvrat
def caclDVRAT(code,date,collection,duration):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(duration+1);
    records = list(records);
    if(len(records)<duration+1):
        return None;
    arr = [];
    count = 0;
    total = len(records);
    while(count<duration):
        if(records[count+1]['close'] is None or records[count+1]['close']==0):
            arr.append(None);
        else:
            arr.append((records[count]['close']-records[count+1]['close'])/records[count+1]['close']);
        count += 1;
    return np.std(arr);

# 稀释每股收�? 净利润/（发行股�?潜在普通股�?
# code: 股票代码
# date: 当前日期
# 暂无数据�?
def caclDEPS(code,date):
    # TODO
    return;

# 波幅中位�? m=ln（当日最高价�?ln（当日最低价
#             求m 3个月内的中位�?
# code: 股票代码
# date: 当前日期
# collection: 股票数据集合
# return dhilo
def caclDHILO(code,date,collection):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(90);
    records = list(records);
    if(len(records)<90):
        return None;
    else:
        arr = [];
        for i in records:
            arr.append(i['high']-i['low']);
        return np.median(arr);

# 产权比率: 负债合�?所有者权�?
# code: 股票代码
# date: 当前日期
# record：当前面数据
# return der
def caclDER(code,date,record):
    debt = record['totalAssets']*10000-record['bvps']*record['totals']*100000000;
    bvps = record['bvps'] * record['totals']*100000000;
    return debt/bvps;

# 下跌波动: 过往12个月中，市场组合日收益为负时，个股日收益标准�?市场组合日收益标准差
# code: 股票代码
# date: 当前日期
# 暂无数据�?
def caclDDNSR(code,date):
    # TODO
    return;

 # 下跌相关系数: 过往12个月中，市场组合日收益为负时，个股日收益与市场组合日收益的相关系�?协方�?
# code: 股票代码
# date: 当前日期
# 暂无数据�?
def caclDDNCR(code,date):
    # TODO
    return;   
# 下跌贝塔: 过往12个月中，市场组合日收益为负时，个股日收益关于市场组合日收益的线性回归系�?
# code: 股票代码
# date: 当前日期
# 暂无数据�?
def caclDDNBT(code,date):
    # TODO
    return;

# DAVOL: n日平均换手率/120日平均换手率    n=5�?0�?0
# code: 股票代码
# date: 当前日期
# collection: 股票数据集合
# duration: 周期
# return davoln
def caclDAVOL(code,date,collection,duration):
    record = collection.find_one({'date':date});
    if(not record or record['tma120'] is None or record['tma120']==0):
        return None;
    else:
        return record['tma'+str(duration)]/record['tma120'];

# 流动比率: 流动资产合计/流动负债合�?
# code: 股票代�??
# date: 当前日期
# recode: 当前面数�?
# return currentratio
def caclCurrentRatio(code,date,record):
    return record['currentratio'];

# 流动资产周转�? 营业收入/流动资产合计
# code: 股票代码
# date: 当前日期
# recode: 当前面数�?
# return currentasset_turnover    
def caclCurrentAssetsTRatio(code,date,record):
    return record['currentasset_turnover'];

# 流动资产比率:流动资产合计/总资�?
# code: 股票代码
# date: 当前日期
# recode: 当前面数�?
# return car    
def caclCurrentAssetsRatio(code,date,record):
    return record['liquidAssets']/(record['totalAssets']*10000);

# 现金流市值比: 每股派现（税前）×分红前总股�?总市�?
# code: 股票代码
# date: 当前日期
# 不明�?
def caclCTP(code,date,record,close):
    # TODO
    return;

# �?4月的累计收益: （当前收盘价-24月前收盘价）/24月前收盘�?
# code: 股票代码
# date: 当前日期
# collection: 股票数据集合
# return cmra
def caclCMRA(code,date,collection):
    # collection = c.history[code];
    end = date;
    ss = date.split('-');
    year = ss[0];
    pyear = int(year)-2;
    begin = str(pyear)+'-'+ss[1]+'-'+ss[2];
    record = collection.find_one({'date':date});
    precord = collection.find({'date':{'$lte':begin}}).sort([("date",mongo.DESCENDING)]).limit(1);
    precord = list(precord);
    if(len(precord)<1 or precord[0]['close']==0):
        return None;
    precord = precord[0];
    return float(record['close']-precord['close'])/precord['close'];

# 现金比率:期末现金及现金等价物余额/流动负债合�?
# code: 股票代码
# date: 当前日期
# recode: 当前面数�?
# return cashratio  
def caclCTCL(code,date,record):
    return record['cashratio'];

# 经营活动产生的现金流量净额与营业收入之比: 经营活动产生的现金流量净�?营业收入
# code: 股票代码
# date: 当前日期
# 暂无数据�?  
def caclCROS(code,date):
    # TODO
    return;

# 应付债券与总资产之�? 应付债券/总资�?
# code: 股票代码
# date: 当前日期
# 暂无数据�?
def caclBPTA(code,date):
    # TODO
    return;

# 账面杠杆: 负�?股东权益
# code: 股票代码
# date: 当前日期
# 暂无数据�?
def caclBLEV(code,date):
    # TODO
    return;

# 对数资产: 资产的对�?
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# return assi
def caclASSI(code,date,record):
    return math.log(record['totalAssets']*10000);

# 应收账款周转�? 营业收入/（应收账�?应收票据+预收账款�?
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# return arturnover    
def caclATRRate(code,date,record):
    return record['arturnover'];

# 应收账款周转天数: 管理费用/营业总收�?
# code: 股票代码
# date: 当前日期
# record: 当前面数�?
# return arturndays      
def caclATRDays(code,date,record):
    return record['arturndays'];

# Admini Expense Rate: 管理费用/营业总收�?
# code: 股票代码
# date: 当前日期
# 暂无数据�?   
def caclAdminiER(code,date):
    # TODO
    return;

# 应付账款周转�? 营业成本/（应付账�?应付票据+预付款项�?
# code: 股票代码
# date: 当前日期
# 暂无数据�?
def caclAccountPTDays(code,date):
    # TODO
    return;

# 应付账款周转天数: 360/应付账款周转�?
# code: 股票代码
# date: 当前日期
# 暂无数据�?
def caclAccountPTDays(code,date):
    # TODO
    return;

# 阶段强势指标: A=统计总数(收盘�?开盘价 并且 对应大盘收盘�?对应大盘开盘价,n);
#               B=统计总数(对应大盘收盘�?对应大盘开盘价,n); 
#               JDQS=A/B n=20
# code: 股票代码
# date: 当前日期
# collection: 股票数据集合
# return jdqs
def caclJDQS20(code,date,collection,t):
    records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(20);
    records = list(records);
    grail = None;
    code = code.encode("utf-8");
    if(code.startswith('00') and code.startswith('002')):
        grail = c['daily'+t]['zxb'];
    elif(code.startswith('00')):
        grail = c['daily'+t]['sz'];
    elif(code.startswith('60')):
        grail = c['daily'+t]['sh'];
    elif(code.startswith('30')):
        grail = c['daily'+t]['cyb'];
    grecords = grail.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(20);
    grecords = list(grecords);
    if(len(grecords)<20):
        return None;

    if(len(records)<20):
        return None;
    a = 0;
    b = 0;
    count = 0;
    while(count<20):
        if(grecords[count]['close']<grecords[count]['open']):
            b+=1;
            if(records[count]['close']>records[count]['open']):
                a+=1;
        count += 1;
    if(b<=0):
        return None;
    return a/b;

# 变化率指�? 当日收盘�?n日前收盘�?与REVSn相同
# code: 股票代码
# date: 当前日期
# collection: 股票数据集合
# duration: 周期 这里�?2�?4
# return rcn
# def caclRC(code,date,collection,duration):
#     records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(duration);
#     records = list(records);
#     if(len(records)<duration):
#         return None;
#     if(records[duration-1]['close']<=0):
#         return None;
#     return records[0]['close']/records[duration-1]['close'];

# 正成交量指标:暂无公式
# code: 股票代码
# date: 当前日期
def caclPVI(code,date):
    # TODO
    return;

# 负成交量指标:暂无公式
# code: 股票代码
# date: 当前日期
def caclNVI(code,date):
    # TODO
    return;

# 艾德透视指标:暂无公式
# code: 股票代码
# date: 当前日期
def caclElder(code,date):
    # TODO
    return;


def updateRecord(c,date,collection):
    update = {};
    tr = caclTR(c,date,collection);
    update.setdefault('atr6',caclATR6loop(c,date,collection,6));
    update.setdefault('atr14',caclATR14loop(c,date,collection,14));
    caclBIAS(c,date,collection);
    update.setdefault('bias60',caclBIAS60(c,date,collection));
    bollup,bolldown = caclBOLLUPDOWN(c,date,collection);
    update.setdefault('bollup',bollup);
    update.setdefault('bolldown',bolldown);
    caclMA88(c,date,collection);
    caclTP(c,date,collection);
    update.setdefault('cci5',caclCCI(c,date,collection,5));
    update.setdefault('cci10',caclCCI(c,date,collection,10));
    update.setdefault('cci20',caclCCI(c,date,collection,20));
    update.setdefault('cci88',caclCCI(c,date,collection,88));
    caclRSV(c,date,collection);
    caclRSVSMA(c,date,collection,3);
    update.setdefault('kdj_k',caclKDJ_K(c,date,collection));
    caclKSMA(c,date,collection,3);
    update.setdefault('kdj_d',caclKDJ_D(c,date,collection));
    caclROC(c,date,collection,6);
    caclROC(c,date,collection,11);
    caclROC(c,date,collection,14);
    caclROC(c,date,collection,20);
    caclUSDDSD(c,date,collection);
    uprvi,downrvi,rvi = caclUPDOWNRVI(c,date,collection);
    update.setdefault('uprvi',uprvi);
    update.setdefault('downrvi',downrvi);
    update.setdefault('rvi',rvi);
    chandeSD,chandeSU,cmo = caclChandeSDSUCMO(c,date,collection,10);
    update.setdefault('chandeSD',chandeSD);
    update.setdefault('chandeSU',chandeSU);
    update.setdefault('cmo',cmo);
    caclBIASDIF(c,date,collection);
    dbcd = caclDBCD(c,date,collection,17);
    update.setdefault('dbcd',dbcd);
    caclOBV(c,date,collection);
    obvma6 = getOBVMA(c,date,collection,6);
    obvma20 = getOBVMA(c,date,collection,20);
    update.setdefault('obvma6',obvma6);
    update.setdefault('obvma20',obvma20);
    # caclTVMA6(c,date,collection);
    # caclTVMA20(c,date,collection);
    # tvstd6 = caclTVSTD6(c,date,collection);
    # tvstd20 = caclTVSTD20(c,date,collection);
    # update.setdefault('tvstd6',tvstd6);
    # update.setdefault('tvstd20',tvstd20);
    caclVDIFF(c,date,collection);
    vdea,vmacd = caclVDEAVMACD(c,date,collection);
    vosc = caclVOSC(c,date,collection);
    vr = caclVR(c,date,collection);
    vroc6 = caclVROC6(c,date,collection);
    vroc12 = caclVROC12(c,date,collection);
    vstd10 = caclVSTD10(c,date,collection);
    vstd20 = caclVSTD20(c,date,collection);
    ko = caclKO(c,date,collection);
    mfi = caclMFI(c,date,collection,20);
    update.setdefault('vdea',vdea);
    update.setdefault('vmacd',vmacd);
    update.setdefault('vosc',vosc);
    update.setdefault('vr',vr);
    update.setdefault('vroc6',vroc6);
    update.setdefault('vroc12',vroc12);
    update.setdefault('vstd10',vstd10);
    update.setdefault('vstd20',vstd20);
    update.setdefault('ko',ko);
    update.setdefault('mfi',mfi);
    caclAD(c,date,collection);
    adma6 = caclADMA(c,date,collection,6);
    adma20 = caclADMA(c,date,collection,20);
    cc = caclCoppockCurve(c,date,collection);
    update.setdefault('adma6',adma6);
    update.setdefault('adma20',adma20);
    update.setdefault('cc',cc);
    caclSI(c,date,collection);
    asi = caclASI(c,date,collection);
    co = caclCO(c,date,collection);
    update.setdefault('asi',asi);
    update.setdefault('co',co);
    caclHL(c,date,collection);
    cv = caclCV(c,date,collection);
    emv6 = caclEMV(c,date,collection,6);
    emv14 = caclEMV(c,date,collection,14);
    update.setdefault('cv',cv);
    update.setdefault('emv6',emv6);
    update.setdefault('emv14',emv14);
    caclDI(c,date,collection);
    caclADX(c,date,collection);
    adxr = caclADXR(c,date,collection);
    arrondown,arronup = caclArron(c,date,collection);
    update.setdefault('adxr',adxr);
    update.setdefault('arrondown',arrondown);
    update.setdefault('arronup',arronup);
    caclEMA12(c,date,collection);
    caclEMA26(c,date,collection);
    dea,macd = caclDEAMACD(c,date,collection);
    diz,dif = caclDIZF(c,date,collection);
    update.setdefault('dea',dea);
    update.setdefault('macd',macd);
    update.setdefault('diz',diz);
    update.setdefault('dif',dif);
    caclMTM(c,date,collection);
    mtmma = caclMTMMA(c,date,collection);
    update.setdefault('mtmma',mtmma);
    caclPVT(c,date,collection);
    pvt6 = caclPVT6(c,date,collection);
    pvt12 = caclPVT12(c,date,collection);
    update.setdefault('pvt6',pvt6);
    update.setdefault('pvt12',pvt12);
    caclEMAEMA12(c,date,collection);
    caclEMAEMAEMA12(c,date,collection);
    caclTRIX(c,date,collection);
    trixma5 = caclTRIXMA(c,date,collection,5);
    trixma10 = caclTRIXMA(c,date,collection,10);
    uos = caclUOS(c,date,collection);
    mrc6 = caclMA10RegressCoeff6(c,date,collection);
    mrc12 = caclMA10RegressCoeff12(c,date,collection);
    plrc6 = caclPLRC6(c,date,collection);
    plrc12 = caclPLRC12(c,date,collection);
    ri5 = caclRI(c,date,collection,5);
    ri10 = caclRI(c,date,collection,10);
    bbi,bbic = caclBB(c,date,collection);
    tema5 = caclTEMA(c,date,collection,5);
    tema10 = caclTEMA(c,date,collection,10);
    update.setdefault('trixma5',trixma5);
    update.setdefault('trixma10',trixma10);
    update.setdefault('uos',uos);
    update.setdefault('mrc6',mrc6);
    update.setdefault('mrc12',mrc12);
    update.setdefault('plrc6',plrc6);
    update.setdefault('plrc12',plrc12);
    update.setdefault('ri5',ri5);
    update.setdefault('ri10',ri10);
    update.setdefault('bbi',bbi);
    update.setdefault('bbic',bbic);
    update.setdefault('tema5',tema5);
    update.setdefault('tema10',tema10);
    caclAR(c,date,collection);
    caclBR(c,date,collection);
    arbr = caclARBR(c,date,collection);
    cr20 = caclCR20(c,date,collection);
    mi = caclMassIndex(c,date,collection);
    bearpower,bullpower = caclBearBullPower(c,date,collection);
    skewness5 = caclSkewness(c,date,collection,5);
    skewness10 = caclSkewness(c,date,collection,10);
    skewness20 = caclSkewness(c,date,collection,20);
    skewness60 = caclSkewness(c,date,collection,60);
    skewness120 = caclSkewness(c,date,collection,120);
    skewness240 = caclSkewness(c,date,collection,240);
    tma5,volstd5 = caclVolatility(c,date,collection,5);
    tma10,volstd10 = caclVolatility(c,date,collection,10);
    tma20,volstd20 = caclVolatility(c,date,collection,20);
    tma60,volstd60 = caclVolatility(c,date,collection,60);
    tma120,volstd120 = caclVolatility(c,date,collection,120);
    tma240,volstd240 = caclVolatility(c,date,collection,240);
    ftw = caclFTW(c,date,collection);
    # wvad6 = caclWVAD(c,date,collection,6);
    # wvad12 = caclWVAD(c,date,collection,12);
    rsi6 = caclRSI(c,date,collection,6);
    rsi12 = caclRSI(c,date,collection,12);
    rsi24 = caclRSI(c,date,collection,24);
    revs5 = caclREVS(c,date,collection,5);
    revs10 = caclREVS(c,date,collection,10);
    revs12 = caclREVS(c,date,collection,12);
    revs20 = caclREVS(c,date,collection,20);
    revs24 = caclREVS(c,date,collection,24);
    psy6 = caclPSY(c,date,collection,6);
    psy12 = caclPSY(c,date,collection,12);
    # wvadma6 = caclWVADMA6(c,date,collection);
    # wvadma12 = caclWVADMA12(c,date,collection);
    dvrat5 = caclDVRAT(c,date,collection,5);
    dvrat10 = caclDVRAT(c,date,collection,10);
    dvrat20 = caclDVRAT(c,date,collection,20);
    dvrat60 = caclDVRAT(c,date,collection,60);
    dvrat120 = caclDVRAT(c,date,collection,120);
    dhilo = caclDHILO(c,date,collection);
    davol5 = caclDAVOL(c,date,collection,5);
    davol10 = caclDAVOL(c,date,collection,10);
    davol20 = caclDAVOL(c,date,collection,20);
    cmra = caclCMRA(c,date,collection);
    jdqs = caclJDQS20(c,date,collection,'');

    update.setdefault('arbr',arbr);
    update.setdefault('cr20',cr20);
    update.setdefault('mi',mi);
    update.setdefault('bearpower',bearpower);
    update.setdefault('bullpower',bullpower);
    update.setdefault('skewness5',skewness5);
    update.setdefault('skewness10',skewness10);
    update.setdefault('skewness20',skewness20);
    update.setdefault('skewness60',skewness60);
    update.setdefault('skewness120',skewness120);
    update.setdefault('skewness240',skewness240);
    update.setdefault('tma5',tma5);
    update.setdefault('volstd5',volstd5);
    update.setdefault('tma10',tma10);
    update.setdefault('volstd10',volstd10);
    update.setdefault('tma20',tma20);
    update.setdefault('volstd20',volstd20);
    update.setdefault('tma60',tma60);
    update.setdefault('volstd60',volstd60);
    update.setdefault('volstd20',volstd20);
    update.setdefault('tma120',tma120);
    update.setdefault('volstd120',volstd120);
    update.setdefault('tma240',tma240);
    update.setdefault('volstd240',volstd240);
    update.setdefault('ftw',ftw);
    # update.setdefault('wvad6',wvad6);
    # update.setdefault('wvad12',wvad12);
    update.setdefault('rsi6',rsi6);
    update.setdefault('rsi12',rsi12);
    update.setdefault('rsi24',rsi24);
    update.setdefault('revs5',revs5);
    update.setdefault('revs10',revs10);
    update.setdefault('revs12',revs12);
    update.setdefault('revs20',revs20);
    update.setdefault('revs24',revs24);
    update.setdefault('psy6',psy6);
    update.setdefault('psy12',psy12);
    # update.setdefault('wvadma6',wvadma6);
    # update.setdefault('wvadma12',wvadma12);
    update.setdefault('dvrat5',dvrat5);
    update.setdefault('dvrat10',dvrat10);
    update.setdefault('dvrat20',dvrat20);
    update.setdefault('dvrat60',dvrat60);
    update.setdefault('dvrat120',dvrat120);
    update.setdefault('dhilo',dhilo);
    update.setdefault('davol5',davol5);
    update.setdefault('davol10',davol10);
    update.setdefault('davol20',davol20);
    update.setdefault('cmra',cmra);
    crecord = collection.find_one({'date':date});
    collection.update({'_id':crecord['_id']},{'$set':update});

# 计算超额
def caclExprice(code,date):
    records = c.daily5[code].find({'date':{'$lt':date}});
    records = list(records);
    total = len(records);
    count = 1;
    high = records[0]['exprice'];
    low = records[0]['exprice'];
    for record in records:
        if(count==1):
            exopen = record['exprice'];
        if(count == total):
            exclose = record['exprice'];
        if(record['exprice']>high):
            high = record['exprice'];
        if(record['exprice']<low):
            low = record['exprice'];
        count += 1;
    cdaily = c.daily[code].find_one({'date':date});
    c.daily[code].update({'_id':cdaily['_id']},{'$set':{'exopen':exopen,'exclose':exclose,'exhigh':high,'exlow':low}});

# 大数据选股
# 近五日平�?交易�?�?总股本的 2% �?股价 5日内 下跌10%
def caclTop(date):
    codelist = c.db['codelist'].find({});
    c.db.recommand.remove();
    for code in codelist:
        collection = c.daily[code['code']];
        records = collection.find({'date':{'$lte':date}}).sort([("date",mongo.DESCENDING)]).limit(5);
        records = list(records);
        if(len(records)==5):
            record = records[0];
            total = code['totals'] * 100000000;
            if(record['v_ma5']<total*0.02 and records[4]['close']>0):
                if((record['close']-records[4]['close'])/records[4]['close']<-0.1):
                    insert = {};
                    insert.setdefault('code',code['code']);
                    insert.setdefault('name',code['name']);
                    insert.setdefault('price',record['close']);
                    c.db.recommand.insert(insert);
    conn = httplib.HTTPConnection("weixin.aitradeapp.com");
    conn.request('GET','/app/index.php?i=72&c=entry&do=recommand&m=ld_financial&over=1');
    conn.close();