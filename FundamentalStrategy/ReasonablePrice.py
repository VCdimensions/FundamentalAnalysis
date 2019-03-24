# -*- coding: utf-8 -*-
"""
Created on Thu Oct 25 16:04:25 2018

@author: SUNG ANN LEE
"""

import pandas as pd
import numpy as np
import datetime as dt
from pandas.io import sql
from sqlalchemy import create_engine
from dateutil.relativedelta import relativedelta

import FundamentalAnalysis.FundamentalStrategy.LongTermValueStrategy as LongTermValueStrategy
QuarterToMonth = LongTermValueStrategy.QuarterToMonth


def ReasonablePrice(StkNum, Beta, BenchmarkDate = '20180815'):
    
        BenchmarkDate = dt.datetime.strptime(BenchmarkDate,'%Y%m%d').date()
        ReasonablePriceDataFrame = pd.DataFrame(columns = StkNum)
    
        ProfitabilityEngine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4".format('root', 'PASSWORD', '127.0.0.1', 'profitability'))
        ProfitabilityConnection = ProfitabilityEngine.connect()
        
        MonthlyRevenueEngine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4".format('root', 'PASSWORD', '127.0.0.1', 'monthlyrevenue'))
        MonthlyRevenueConnection = MonthlyRevenueEngine.connect()
        
        PEHistoryEngine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4".format('root', 'PASSWORD', '127.0.0.1', 'pehistory'))
        PEHistoryConnection = PEHistoryEngine.connect()
        
        PerformanceEngine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4".format('root', 'PASSWORD', '127.0.0.1', 'performance'))
        PerformanceConnection = PerformanceEngine.connect()

        StockPriceEngine = create_engine("mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4".format('root', 'PASSWORD', '127.0.0.1', 'stockprice'))
        StockPriceConnection = StockPriceEngine.connect()
        
        for ID in StkNum:
            
            EndDate = dt.date(BenchmarkDate.year, 12, 31) 
            StartDate = dt.date(BenchmarkDate.year, 1, 1) 

            #Profitability 今年的營業利益率平均/去年營業利益率平均 = 今年預估營業利益率的YOY
            ProfitabilityTable = sql.read_sql('select * from '+ '`'+str(ID)+'new'+'`' +';',con= ProfitabilityConnection)
            ProfitabilityTable = ProfitabilityTable.drop_duplicates()
            ProfitabilityTable = ProfitabilityTable.set_index('Date').sort_index(ascending = False)
            ProfitabilityTable.index = [dt.datetime.strptime(QuarterToMonth(ProfitabilityTable.index)[i],'%Y%m%d').date() for i in range(0,len(ProfitabilityTable.index))]
            
            ThisYearOperatingProfitRatio = ProfitabilityTable.loc[EndDate:StartDate]['OperatingProfitRatio'].mean()
            LastYearOperatingProfitRatio = ProfitabilityTable.loc[EndDate - relativedelta(years=1):StartDate - relativedelta(years=1)]['OperatingProfitRatio'].mean()
            
            OperatingAvgYoY = (ThisYearOperatingProfitRatio - LastYearOperatingProfitRatio)/LastYearOperatingProfitRatio
            
            #MonthlyRevenue 今年的營收平均YoY(去掉Max因為過年關係) = 今年預估營收成長率YoY
            MonthlyRevenueTable = sql.read_sql('select * from '+ '`'+str(ID)+'new'+'`' +';',con= MonthlyRevenueConnection)
            MonthlyRevenueTable = MonthlyRevenueTable.drop_duplicates()
            MonthlyRevenueTable = MonthlyRevenueTable.set_index('Date').sort_index(ascending = False)
            Year = list(np.array(list(map(int,[MonthlyRevenueTable.index[i][0:3] for i in range(0,len(MonthlyRevenueTable.index))]))) + 1911)
            MonthlyRevenueTable.index = [str(Year[i])+str(MonthlyRevenueTable.index[i][-2:]) for i in range(0,len(MonthlyRevenueTable.index))]
            MonthlyRevenueTable.index = [dt.datetime.strptime(MonthlyRevenueTable.index[i],'%Y%m').date() for i in range(0,len(MonthlyRevenueTable.index))]
            
            ThisYearAvgMonthlyRevenueYoY = MonthlyRevenueTable.loc[EndDate:StartDate]
            
            RevenueAvgYoY = ThisYearAvgMonthlyRevenueYoY.drop(ThisYearAvgMonthlyRevenueYoY['YoYGrowth'].idxmax())['YoYGrowth'].mean()
            
            #PEhistory 近三年最低的PE ratio
            PEHistoryTable = sql.read_sql('select * from '+ '`'+str(ID)+'new'+'`' +';',con= PEHistoryConnection)
            PEHistoryTable = PEHistoryTable.drop_duplicates()
            PEHistoryTable = PEHistoryTable.set_index('Year')
            PEHistoryTable.index = [str(int(list(PEHistoryTable.index)[i])+1911) for i in range(0,len(PEHistoryTable.index))]
            PEHistoryTable.index = [dt.datetime.strptime(PEHistoryTable.index[i],'%Y').date() for i in range(0,len(PEHistoryTable.index))]
            
            MinimumPE  = PEHistoryTable.loc[EndDate:StartDate - relativedelta(years = 3)]['MinPE'].min()
            
            #Performance 
            PerformanceTable = sql.read_sql('select * from '+ '`'+str(ID)+'new'+'`' +';',con= PerformanceConnection)
            PerformanceTable = PerformanceTable.drop_duplicates()
            PerformanceTable = PerformanceTable.set_index('Date').sort_index(ascending = False)
            PerformanceTable.index = [dt.datetime.strptime(QuarterToMonth(PerformanceTable.index)[i],'%Y%m%d').date() for i in range(0,len(PerformanceTable.index))]

            LastYearEPS = PerformanceTable.loc[EndDate - relativedelta(years=1):StartDate - relativedelta(years=1)]['EPS'].sum()
            
            #StockPrice
            StockPriceTable = sql.read_sql('select * from '+ '`'+str(ID)+'new'+'`' +';',con= StockPriceConnection)
            StockPriceTable = StockPriceTable.drop_duplicates()
            StockPriceTable = StockPriceTable.sort_values('Date',ascending=False)
            StockPriceTable = StockPriceTable.set_index('Date')
            Strptime = list(map(str,[StockPriceTable.index[i] for i in range(0,len(StockPriceTable.index))]))
            Date = [dt.datetime.strptime(Strptime[i][0:10],'%Y-%m-%d').date() for i in range(0,len(Strptime))]
            StockPriceTable.index = Date
            StockPriceTable = StockPriceTable.apply(pd.to_numeric, errors='coerce')
            
            LatestStockPrice = StockPriceTable['AdjClose'].head(1)[0]
            
            ReasonablePriceDataFrame.loc['LatestStockPrice',str(ID)] = LatestStockPrice
            ReasonablePriceDataFrame.loc['ReasonableStockPrice',str(ID)] = LastYearEPS * (1 + RevenueAvgYoY) * (1 + OperatingAvgYoY) * MinimumPE * Beta
            ReasonablePriceDataFrame.loc['Reasonable%',str(ID)] = ( (LastYearEPS * (1 + RevenueAvgYoY) * (1 + OperatingAvgYoY) * MinimumPE * Beta) - LatestStockPrice) / LatestStockPrice
        
        ProfitabilityConnection.close()
        MonthlyRevenueConnection.close()
        PEHistoryConnection.close()
        PerformanceConnection.close()
        StockPriceConnection.close()

        return(ReasonablePriceDataFrame)


