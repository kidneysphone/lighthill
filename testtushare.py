import tushare as ts 
df=ts.get_hist_data('002019',ktype='5',start ='2017-03-01',end= '2017-03-02')
print df 
