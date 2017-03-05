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
CONN_ADDR1 = 's-m5e9656f4072c8d4.mongodb.rds.aliyuncs.com:3717'
CONN_ADDR2 = 's-m5e7370f67573e34.mongodb.rds.aliyuncs.com:3717'
username = 'root'
password = 'Liangdian123'
#获取mongoclient
c = mongo.MongoClient([CONN_ADDR1, CONN_ADDR2])
#授权. 这里的user基于admin数据库授�?
c.admin.authenticate(username, password)quarter  = c.quarter['history'];
def getallcode():
    arr = []
    try:
        codea = c.db.codelist.find({'timeToMarket':{'$gt':0}}).sort('code')
        # print codea
        for item in codea:
            arr.append(item['code'])
    except Exception,e:
        print Exception,":",e
    return arr;
codelist=getallcode();
qua = c.quarter;
now = datetime.datetime.now();
year = int(now.strftime("%Y"));
month = int(now.strftime("%m"));
if(month<3 or (month>=10 and month<=12)):
    q = 4;
elif(month<4):
    q = 1;
elif(month<8):
    q = 2;
elif(month<10):
    q = 3;
base = ts.get_stock_basics();
reports = None;
profits = None;
operations = None;
growths = None;
debtpayings = None;
cashflows = None;
# step1: data
# q=3;
reports = ts.get_report_data(year,q);
profits = ts.get_profit_data(year,q);
operations = ts.get_operation_data(year,q);
growths = ts.get_growth_data(year,q);
debtpayings = ts.get_debtpaying_data(year,q);
cashflows = ts.get_cashflow_data(year,q);
for code in codelist:
	codeinfo = c.db.codelist.find_one({'code':code});
	if(reports is None):
		report = None;
	else:
		report = reports[reports.code==code];
	if(profits is None):
		profit = None;
	else:
		profit = profits[profits.code==code];
	if(operations is None):
		operation = None;
	else:
		operation = operations[operations.code==code];
	if(growths is None):
		growth = None;
	else:
		growth = growths[growths.code==code];
	if(debtpayings is None):
		debtpaying = None;
	else:
		debtpaying = debtpayings[debtpayings.code==code];
	if(cashflows is None):
		cashflow = None;
	else:
		cashflow = cashflows[cashflows.code==code];
	arr = {};
	arr.setdefault('year',year);
	arr.setdefault('quarter',q);
	arr.setdefault('pe',codeinfo['pe']);
	arr.setdefault('outstanding',codeinfo['outstanding']);
	arr.setdefault('totals',codeinfo['totals']);
	arr.setdefault('totalAssets',codeinfo['totalAssets']);
	arr.setdefault('liquidAssets',codeinfo['liquidAssets']);
	arr.setdefault('fixedAssets',codeinfo['fixedAssets']);
	arr.setdefault('pb',codeinfo['pb']);
	arr.setdefault('profit',codeinfo['profit']);
	arr.setdefault('npr',codeinfo['npr']);
	arr.setdefault('holders',codeinfo['holders']);
	if(report is not None and not report.empty):

		arr.setdefault('eps',report['eps'].values[0]);
		arr.setdefault('eps_yoy',report['eps_yoy'].values[0]);
		arr.setdefault('bvps',report['bvps'].values[0]);
		arr.setdefault('roe',report['roe'].values[0]);
		arr.setdefault('epcf',report['epcf'].values[0]);
		arr.setdefault('net_profits',report['net_profits'].values[0]);
		arr.setdefault('profits_yoy',report['profits_yoy'].values[0]);
		arr.setdefault('report_date',report['report_date'].values[0]);
	if(profit is not None and not profit.empty):
		arr.setdefault('roe',profit['roe'].values[0]);
		arr.setdefault('net_profit_ratio',profit['net_profit_ratio'].values[0]);
		arr.setdefault('gross_profit_rate',profit['gross_profit_rate'].values[0]);
		arr.setdefault('net_profits',profit['net_profits'].values[0]);
		arr.setdefault('business_income',profit['business_income'].values[0]);
		arr.setdefault('bips',profit['bips'].values[0]);
	if(operation is not None and not operation.empty):
		arr.setdefault('arturnover',operation['arturnover'].values[0]);
		arr.setdefault('arturndays',operation['arturndays'].values[0]);
		arr.setdefault('inventory_turnover',operation['inventory_turnover'].values[0]);
		arr.setdefault('inventory_days',operation['inventory_days'].values[0]);
		arr.setdefault('currentasset_turnover',operation['currentasset_turnover'].values[0]);
		arr.setdefault('currentasset_days',operation['currentasset_days'].values[0]);
	if(growth is not None and not growth.empty):
		arr.setdefault('mbrg',growth['mbrg'].values[0]);
		arr.setdefault('nprg',growth['nprg'].values[0]);
		arr.setdefault('nav',growth['nav'].values[0]);
		arr.setdefault('targ',growth['targ'].values[0]);
		arr.setdefault('epsg',growth['epsg'].values[0]);
		arr.setdefault('seg',growth['seg'].values[0]);
	if(debtpaying is not None and not debtpaying.empty):
		arr.setdefault('currentratio',debtpaying['currentratio'].values[0]);
		arr.setdefault('quickratio',debtpaying['quickratio'].values[0]);
		arr.setdefault('cashratio',debtpaying['cashratio'].values[0]);
		arr.setdefault('icratio',debtpaying['icratio'].values[0]);
		arr.setdefault('sheqratio',debtpaying['sheqratio'].values[0]);
		arr.setdefault('adratio',debtpaying['adratio'].values[0]);
	if(cashflow is not None and not cashflow.empty):
		arr.setdefault('cf_sales',cashflow['cf_sales'].values[0]);
		arr.setdefault('rateofreturn',cashflow['rateofreturn'].values[0]);
		arr.setdefault('cf_nm',cashflow['cf_nm'].values[0]);
		arr.setdefault('cf_liabilities',cashflow['cf_liabilities'].values[0]);
		arr.setdefault('cashflowratio',cashflow['cashflowratio'].values[0]);
	qua[code].insert(arr);
# step2: update data
for code in codelist:
	records = qua[code].find({}).sort([("year",mongo.ASCENDING),("quarter",mongo.ASCENDING)]);
	for record in records:
		date = str(record['year'])+'-'+record['report_date'];
		month = int(date.split('-')[1])-1;
		earnmom = methods.caclEARNMOM(code,date);
		degm = methods.caclDEGM(code,date);
		acca = methods.caclACCA(code,date,record);
		close = c.daily[code].find_one({'date':{'$lt':str(record['year'])+'-'+str(month)+'-01'}});
		close = close['close'];
		if(close is not None):
			cfo2ev = methods.caclCFO2EV(code,date,record,close);
			ta2ev = methods.caclTA2EV(code,date,record,close);
			ps = methods.caclPS(code,date,record,close);
			pcf = methods.caclPCF(code,date,record,close);
			mlev = methods.caclMLEV(code,date,record,close);
			tdta = methods.caclTDTA(code,date,record,close);
			lflo = methods.caclLFLO(code,date,record,close);
			lcap = methods.caclLCAP(code,date,record,close);
			etp = methods.caclETP(code,date,record,close);
		else:
			cfo2ev = None;
			ta2ev = None;
			ps = None;
			pcf = None;
			mlev = None;
			tdta = None;
			lflo = None;
			lcap = None;
			etp = None;
		tpgr = methods.caclTPGR(code,date);
		tatr = methods.caclTATR(code,date,record);
		sue = methods.caclSUE(code,date);
		roe1 = methods.caclROE(code,date,1);
		roe5 = methods.caclROE(code,date,5);
		roa1 = methods.caclROA(code,date,1);
		roa5 = methods.caclROA(code,date,5);
		
		ocgr = methods.caclOCGR(code,date);
		orgr = methods.caclORGR(code,date);
		nptotor = methods.caclNPtoTOR(code,date,record);
		ncar = methods.caclNCAR(code,date,record);
		npr = methods.caclNPR(code,date,record);
		
		far = methods.caclFAR(code,date,record);
		lar = methods.caclLAR(code,date,record);
		
		etr = methods.caclETR(code,date,record);
		efar = methods.caclEFAR(code,date,record);
		egro = methods.caclEGRO(code,date);
		der = methods.caclDER(code,date,record);
		car = methods.caclCurrentAssetsRatio(code,date,record);
		assi = methods.caclASSI(code,date,record);
		update = {};
		update.setdefault('earnmom',earnmom);
		update.setdefault('degm',degm);
		update.setdefault('acca',acca);
		update.setdefault('cfo2ev',cfo2ev);
		update.setdefault('ta2ev',ta2ev);
		update.setdefault('tpgr',tpgr);
		update.setdefault('tatr',tatr);
		update.setdefault('sue',sue);
		update.setdefault('roe1',roe1);
		update.setdefault('roe5',roe5);
		update.setdefault('roa1',roa1);
		update.setdefault('roa5',roa5);
		update.setdefault('ps',ps);
		update.setdefault('pcf',pcf);
		update.setdefault('ocgr',ocgr);
		update.setdefault('orgr',orgr);
		update.setdefault('nptotor',nptotor);
		update.setdefault('ncar',ncar);
		update.setdefault('npr',npr);
		update.setdefault('mlev',mlev);
		update.setdefault('tdta',tdta);
		update.setdefault('lflo',lflo);
		update.setdefault('lcap',lcap);
		update.setdefault('far',far);
		update.setdefault('lar',lar);
		update.setdefault('etp',etp);
		update.setdefault('etr',etr);
		update.setdefault('efar',efar);
		update.setdefault('egro',egro);
		update.setdefault('der',der);
		update.setdefault('car',car);
		update.setdefault('assi',assi);
		qua[code].update({'_id':record['_id']},{'$set':update});
	break;
