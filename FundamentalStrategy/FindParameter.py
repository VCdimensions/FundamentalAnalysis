# -*- coding: utf-8 -*-
"""
Created on Thu Oct  4 13:30:09 2018

How to determine the parameter of SelctStock function 
@author: SUNG ANN LEE
"""

import pandas as pd
import numpy as np
import datetime as dt
import FundamentalAnalysis.CrawlDataFromWeb.FinancialStatement as Crawler
import pymysql
from pandas.io import sql
from time import gmtime, strftime
from dateutil.relativedelta import relativedelta



class FindParameter:

    def __init__(self, MonthBack = 6, NumOfStock = 100):
        
        self.MonthBack = MonthBack
        self.NumOfStock = NumOfStock 
        self.StockID = Crawler.FinancialStatement.StockNumber(KYDelete=False)['StkNum'].replace(' ','',regex=True)
        self.TopReturnStock = pd.DataFrame(columns=self.StockID)
        
    def ByTopPriceReturn(self):
        
        StockPriceTableConnection = pymysql.connect(
                host='127.0.0.1',
                user='root',
                passwd='PASSWORD',
                db='stockprice',
                charset='BIG5',
                local_infile=True
                )            
        for ID in self.StockID:
        
            try:
                StockPriceTable = sql.read_sql('select * from '+ '`'+str(ID)+'new'+'`' +';',con= StockPriceTableConnection)
                StockPriceTable = StockPriceTable.drop_duplicates()
            except sql.DatabaseError:
                print(str(ID)+'\t'+'do not exist in StockPriceTable Database')
                continue      
            if len(StockPriceTable) <=3: #資料不存在
                print(str(ID)+'\t'+'StockPriceTable DataFrame is empty')
                continue                    
            
            StockPriceTable = StockPriceTable.sort_values('Date')
            StockPriceTable = StockPriceTable.set_index('Date')
            Strptime = list(map(str,[StockPriceTable.index[i] for i in range(0,len(StockPriceTable.index))]))
            Date = [dt.datetime.strptime(Strptime[i][0:10],'%Y-%m-%d').date() for i in range(0,len(Strptime))]
            StockPriceTable.index = Date
            StockPriceTable = StockPriceTable.convert_objects(convert_numeric=True)
            
            t = strftime("%Y%m%d", gmtime())
            EndDate = dt.date(int(t[0:4]),int(t[4:6]),1) 
            StartDate = EndDate + relativedelta(months =-self.MonthBack)
            
            StockPrice = StockPriceTable[[StockPriceTable.index[i] >= StartDate  for i in range(0,len(StockPriceTable.index))]]
            StockPrice = StockPrice[[StockPrice.index[i] <= EndDate  for i in range(0,len(StockPrice.index))]]
    
            if len(StockPrice) == 0 :
                print(str(ID)+'\tStockPrice is too short')
                continue
            
            tail = StockPrice.tail(1)['AdjClose']
            head = StockPrice.head(1)['AdjClose']

            StockReturn = (tail.values-head.values)/head.values
            
            self.TopReturnStock.loc['Return',str(ID)] = float(StockReturn)
            
            print(str(ID)+'\t'+'has Done')
        
        self.TopReturnStock = self.TopReturnStock.transpose()
        self.TopReturnStock.sort_values('Return',ascending = False, inplace=True)
        
        self.TopReturnStock.head(self.NumOfStock).index # 利用找到的前NumOfStock的股票資料來找尋參數
        

        StockPriceTableConnection.close()
        
    def __str__(self):
        return()
