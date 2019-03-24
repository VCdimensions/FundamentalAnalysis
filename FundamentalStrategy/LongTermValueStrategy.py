# -*- coding: utf-8 -*-
"""
Created on Tue Feb 28 19:06:34 2017

@author: SUNG ANN LEE
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Feb 23 15:34:45 2017

@author: SUNG ANN LEE
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Feb 16 13:46:31 2017

@author: SUNG ANN LEE
"""

import FundamentalAnalysis.CrawlDataFromWeb.FinancialStatement as Crawler 
import FundamentalAnalysis.CrawlDataFromWeb.UpdateDataToMySQL as UpdateData

from pandas.io import sql
import pymysql
import datetime as dt
import pandas as pd
import numpy as np
import math
from dateutil.relativedelta import relativedelta
from time import gmtime, strftime


'''
Convert Quarter Timestamp into Month
'''
def QuarterToMonth(Quarter):
    Data = Quarter.map(str)
    Data = pd.Series(Data)
    Data = Data.map(str)
    Data = [str(int(Data[:-2])+1911)+Data[-2:]+'01' for Data in Data]
    return(Data)




class LongTermValueStrategy:
    
    def __init__(self, BenchmarkDate, UpdateMethod='replace', 
                 OperatingProfitRatio_Std_Benchmark = 0.04,
                 InventoryTurnover_Std_Benchmark=0.3 ,
                 RevenueYoYIsLessThanZeroNum_Benchmark = 1,
                 RevenueYoYGrowthRate_Benchmark = 0,
                 RevenueAeverageGrowthRateMonth_Benchmark = 13,
                 RevenueAntiRate = .2,
                 MinMonthsNum = 12,
                 MarketValue = 50):
        ###Crawl All Listed Stock and Delete KY- Stock
        self.StockID = Crawler.FinancialStatement.StockNumber(KYDelete=True)['StkNum'].replace(' ','',regex=True)
        ###Create A Signal DataFrame
        self.SignalDataFrame = pd.DataFrame(columns=self.StockID)
        ###Create A Information DataFrame
        self.InformationDataFrame = pd.DataFrame(columns=self.StockID)
        ###BenchmarkDate
        self.BenchmarkDate = dt.datetime.strptime(BenchmarkDate,'%Y%m%d').date()   
        self.UpdateMethod = UpdateMethod
        self.OperatingProfitRatio_Std_Benchmark = OperatingProfitRatio_Std_Benchmark 
        self.InventoryTurnover_Std_Benchmark = InventoryTurnover_Std_Benchmark
        self.RevenueYoYIsLessThanZeroNum_Benchmark = RevenueYoYIsLessThanZeroNum_Benchmark 
        self.RevenueYoYGrowthRate_Benchmark = RevenueYoYGrowthRate_Benchmark
        self.RevenueAeverageGrowthRateMonth_Benchmark = RevenueAeverageGrowthRateMonth_Benchmark
        self.RevenueAntiRate = RevenueAntiRate
        self.MinMonthsNum = MinMonthsNum
        self.MarketValue = MarketValue
        
    '''
    Update Data
    '''
    ###Update Quartery Data
    def UpdateQuarteryData(self):
        Update = UpdateData.UpdateDataToMySQL(self.UpdateMethod)
        Update.CashflowTable()
        Update.FinancialRatioTable()
        Update.PerformanceTable()
        Update.ProfitabilityTable()
    ###Update Monthy Data
    def UpdateMonthyData(self):
        Update = UpdateData.UpdateDataToMySQL(self.UpdateMethod)
        Update.MonthlyRevenueTable()
    ###Update Daily Data
    def UpdateCompanyInfo(self):
        Update = UpdateData.UpdateDataToMySQL(self.UpdateMethod)
        Update.InformationAndPEHsitory()
    def UpdateStockPrice(self):
        Update = UpdateData.UpdateDataToMySQL(self.UpdateMethod)
        Update.StockPriceTable()
    
    
    def ProfitablilitySignal(self):
        '''
        Profitability Table
        Q1 OperatingProfitRatio is stable near 8 quarter?
        Q2 4 Year Cumulative Net Income is Positive?
        '''
        ProfitabilityConnection = pymysql.connect(
                        host='127.0.0.1',
                        user='root',
                        passwd='PASSWORD',
                        db='profitability',
                        charset='BIG5',
                        local_infile=True
                        )            
        
        for ID in self.StockID:
            try:
                ProfitabilityTable = sql.read_sql('select * from '+ '`'+str(ID)+'new'+'`' +';',con= ProfitabilityConnection)
                ProfitabilityTable = ProfitabilityTable.drop_duplicates()
            except sql.DatabaseError:
                print(str(ID)+'\t'+'do not exist in Profitability Database')
                continue
            if len(ProfitabilityTable ) <=3: #資料不存在
                print(str(ID)+'\t'+'Profitability DataFrame is empty')
                continue                    
            ProfitabilityTable = ProfitabilityTable.set_index('Date').sort_index(ascending = False)
            ProfitabilityTable.index = [dt.datetime.strptime(QuarterToMonth(ProfitabilityTable.index)[i],'%Y%m%d').date() for i in range(0,len(ProfitabilityTable.index))]
            
            ###Q1 OperatingProfitRatio is stable near 8 quarter?
            #Q1StartDate = self.BenchmarkDate
            #Q1EndDate = dt.date(self.BenchmarkDate.year+2,self.BenchmarkDate.month,self.BenchmarkDate.day) #看兩年的營益率資料
            Q1EndDate = dt.date(self.BenchmarkDate.year,self.BenchmarkDate.month,self.BenchmarkDate.day) - relativedelta(months = 4 ,days=14) #看兩年的營益率資料
            Q1StartDate = Q1EndDate - relativedelta(months=23)
            
            OperatingProfitRatio = ProfitabilityTable[[ProfitabilityTable.index[i] > Q1StartDate  for i in range(0,len(ProfitabilityTable.index))]]
            OperatingProfitRatio = OperatingProfitRatio[[OperatingProfitRatio.index[i] <= Q1EndDate  for i in range(0,len(OperatingProfitRatio.index))]]
            OperatingProfitRatio = OperatingProfitRatio['OperatingProfitRatio']
            
            if len(OperatingProfitRatio) <8 or all([i == None for i in OperatingProfitRatio.tolist() ]) or all(pd.isna(OperatingProfitRatio.tolist())): #少於兩年的營益率資料為False
                print(str(ID)+'\t'+'OperatingProfitRatio is less than 8 Quarters')
                continue
            else:
                OperatingProfitRatio_Std = np.std(OperatingProfitRatio)
                head = np.nanmean(OperatingProfitRatio.head(4))
                tail = np.nanmean(OperatingProfitRatio.tail(4))
            
            if (OperatingProfitRatio_Std <= self.OperatingProfitRatio_Std_Benchmark) or head-tail > self.OperatingProfitRatio_Std_Benchmark*1.4:
                self.SignalDataFrame.loc['ProfitRatioIsStable',str(ID)] = True
            elif head-tail < self.OperatingProfitRatio_Std_Benchmark*2:
                self.SignalDataFrame.loc['ProfitRatioIsStable',str(ID)] = False
            else:
                self.SignalDataFrame.loc['ProfitRatioIsStable',str(ID)] = False
                
            ###Q2 4 Year Cumulative Net Income is Positive?
            Q2EndDate =  dt.date(self.BenchmarkDate.year,self.BenchmarkDate.month,self.BenchmarkDate.day) - relativedelta(months = 4 ,days=14) #看4年的NI資料
            Q2StartDate = Q2EndDate - relativedelta(months=47) #看4年的NI資料
            
            NI = ProfitabilityTable[[ProfitabilityTable.index[i] > Q2StartDate  for i in range(0,len(ProfitabilityTable.index))]]
            NI = NI[[NI.index[i] <= Q2EndDate  for i in range(0,len(NI.index))]]  
            NI = NI['NetIncome']
            
            if len(NI) < 13: #至少要有3年的NI資料
                print(str(ID)+'\t'+'NI is less than 16 Quarters')
                continue
            
            if all(NI>0):
                self.SignalDataFrame.loc['CumulativeNIIsPositive',str(ID)] = True
            else:
                self.SignalDataFrame.loc['CumulativeNIIsPositive',str(ID)] = False
                            
        ProfitabilityConnection.close()
        print('Profitability Table is Done!!!!!')

        
    def FinancialRatioSignal(self): 
             
        '''
        FinancialRatio
        Q1 How about the Inventory Turnover is declinging in 2 years?
        '''
        
        FinancialRatioConnection = pymysql.connect(
                        host='127.0.0.1',
                        user='root',
                        passwd='PASSWORD',
                        db='financialratio',
                        charset='BIG5',
                        local_infile=True
                        )            
        
        for ID in self.StockID:
            if ID == '6538':
                continue #這支股票日期資料有問題
            try:
                FinancialRatioTable = sql.read_sql('select * from '+ '`'+str(ID)+'new'+'`' +';',con= FinancialRatioConnection)
                FinancialRatioTable = FinancialRatioTable.drop_duplicates()
            except sql.DatabaseError:
                print(str(ID)+'\t'+'do not exist in FinancialRatio Database')
                continue
            if len(FinancialRatioTable ) <=3: #資料不存在
                print(str(ID)+'\t'+'FinancialRatio DataFrame is empty')
                continue                    
            FinancialRatioTable = FinancialRatioTable.set_index('index').sort_index(ascending = False)
            FinancialRatioTable.index = [dt.datetime.strptime(QuarterToMonth(FinancialRatioTable.index)[i],'%Y%m%d').date() for i in range(0,len(FinancialRatioTable.index))]
            
            ###Q1 How about the Inventory Turnover is declinging in 2 years?
            Q1EndDate = dt.date(self.BenchmarkDate.year,self.BenchmarkDate.month,self.BenchmarkDate.day) - relativedelta(months = 4 ,days=14)
            Q1StartDate = Q1EndDate - relativedelta(months=23)
            
            InventoryTurnover = FinancialRatioTable[[FinancialRatioTable.index[i] > Q1StartDate  for i in range(0,len(FinancialRatioTable.index))]]
            InventoryTurnover = InventoryTurnover[[InventoryTurnover.index[i] <= Q1EndDate  for i in range(0,len(InventoryTurnover.index))]]
            
            try:
                InventoryTurnover = InventoryTurnover['存貨週轉率次']
            except KeyError:
                print(str(ID)+'\t'+'InventoryTurnover has KeyError')
                continue
                
            if len(InventoryTurnover) <8 or all(pd.isnull(InventoryTurnover) == True) : #少於兩年的存貨週轉率為False
                print(str(ID)+'\t'+'OperatingProfitRatio is less than 8 Quarters')
                continue
            else:
                InventoryTurnover_Std = np.std(InventoryTurnover)
                head = np.nanmean(InventoryTurnover.head(4))
                tail = np.nanmean(InventoryTurnover.tail(4))
            
            if InventoryTurnover_Std <= self.InventoryTurnover_Std_Benchmark  or head - tail > self.InventoryTurnover_Std_Benchmark*.5:
                self.SignalDataFrame.loc['InventoryTurnoverIsSatble',str(ID)] = True
            else:
                self.SignalDataFrame.loc['InventoryTurnoverIsSatble',str(ID)] = False
                
        FinancialRatioConnection.close()
        print('FinancialRatioTable is Done!!!!!')
        

    def MonthlyRevenueSignal(self):            
        '''
        Monthly Revenue
        Q1 YoY Revenue Growth near 12 Month is not zero?
        Q2 What's the Revenue Rate of Growth in One Year?
        '''
        MonthlyRevenueConnection = pymysql.connect(
                        host='127.0.0.1',
                        user='root',
                        passwd='PASSWORD',
                        db='monthlyrevenue',
                        charset='BIG5',
                        local_infile=True
                        )            
        
        for ID in self.StockID:
            try:
                MonthlyRevenueTable = sql.read_sql('select * from '+ '`'+str(ID)+'new'+'`' +';',con= MonthlyRevenueConnection)
                MonthlyRevenueTable = MonthlyRevenueTable.drop_duplicates()
            except sql.DatabaseError:
                print(str(ID)+'\t'+'do not exist in MonthlyRevenue Database')
                continue
            if len(MonthlyRevenueTable ) <=3: #資料不存在
                print(str(ID)+'\t'+'MonthlyRevenue DataFrame is empty')
                continue                    
            MonthlyRevenueTable = MonthlyRevenueTable.set_index('Date').sort_index(ascending = False)
            Year = list(np.array(list(map(int,[MonthlyRevenueTable.index[i][0:3] for i in range(0,len(MonthlyRevenueTable.index))]))) + 1911)
            MonthlyRevenueTable.index = [str(Year[i])+str(MonthlyRevenueTable.index[i][-2:]) for i in range(0,len(MonthlyRevenueTable.index))]
            MonthlyRevenueTable.index = [dt.datetime.strptime(MonthlyRevenueTable.index[i],'%Y%m').date() for i in range(0,len(MonthlyRevenueTable.index))]
            
            #t = strftime("%Y%m%d", gmtime())
            #Q1EndDate = dt.date(int(t[0:4]),int(t[4:6]),1) 
            #Q1StartDate = Q1EndDate + relativedelta(months =-13)
            Q1EndDate = dt.date(self.BenchmarkDate.year,self.BenchmarkDate.month,self.BenchmarkDate.day) #看兩年的營益率資料
            Q1StartDate = Q1EndDate - relativedelta(months=13)

            
#            ###Q1 YoY Revenue Growth near 12 Month is not zero?
#            if self.BenchmarkDate.month == 10: 
#                Q1EndDate = dt.date(self.BenchmarkDate.year+3,3,self.BenchmarkDate.day) #第四季營收在隔年3月31以前公佈
#                Q1StartDate = dt.date(self.BenchmarkDate.year+2,3,self.BenchmarkDate.day)
#            elif self.BenchmarkDate.month == 1:
#                Q1EndDate = dt.date(self.BenchmarkDate.year+2,5,self.BenchmarkDate.day) #第一季營收在5月15以前公佈
#                Q1StartDate = dt.date(self.BenchmarkDate.year+1,5,self.BenchmarkDate.day)
#            elif self.BenchmarkDate.month == 4:
#                Q1EndDate = dt.date(self.BenchmarkDate.year+2,8,self.BenchmarkDate.day) #第二季營收在8月14以前公佈
#                Q1StartDate = dt.date(self.BenchmarkDate.year+1,8,self.BenchmarkDate.day)
#            elif self.BenchmarkDate.month == 7:
#                Q1EndDate = dt.date(self.BenchmarkDate.year+2,11,self.BenchmarkDate.day) #第三季營收在11月14以前公佈
#                Q1StartDate = dt.date(self.BenchmarkDate.year+1,11,self.BenchmarkDate.day)
                
            
            MonthlyRevenue = MonthlyRevenueTable[[MonthlyRevenueTable.index[i] >= Q1StartDate  for i in range(0,len(MonthlyRevenueTable.index))]]
            MonthlyRevenue = MonthlyRevenue[[MonthlyRevenue.index[i] <= Q1EndDate  for i in range(0,len(MonthlyRevenue.index))]]
            
            RevenueYoYGrowth = MonthlyRevenue['YoYGrowth']
            
            head = MonthlyRevenue['YoYCumulativeGrowth'].head(1)[0]
            tail = MonthlyRevenue['YoYCumulativeGrowth'].tail(1)[0]

            #t = strftime("%Y%m%d", gmtime())
            #Q2EndDate = dt.date(int(t[0:4]),int(t[4:6]),1) 
            #Q2StartDate = Q1EndDate + relativedelta(months =- self.RevenueAeverageGrowthRateMonth_Benchmark)
            Q2EndDate = dt.date(self.BenchmarkDate.year,self.BenchmarkDate.month,self.BenchmarkDate.day) #看兩年的營益率資料
            Q2StartDate = Q1EndDate - relativedelta(months=13)
            
            
#            ###Q2 What's the Revenue Rate of Growth in One Year?
#            if self.BenchmarkDate.month == 10: 
#                Q2EndDate = dt.date(self.BenchmarkDate.year+3,3,self.BenchmarkDate.day) #第四季營收在隔年3月31以前公佈
#                Q2StartDate = Q2EndDate + relativedelta(months =- self.RevenueAeverageGrowthRateMonth_Benchmark)
#            elif self.BenchmarkDate.month == 1:
#                Q2EndDate = dt.date(self.BenchmarkDate.year+2,5,self.BenchmarkDate.day) #第一季營收在5月15以前公佈
#                Q2StartDate = Q2EndDate + relativedelta(months =- self.RevenueAeverageGrowthRateMonth_Benchmark)
#            elif self.BenchmarkDate.month == 4:
#                Q2EndDate = dt.date(self.BenchmarkDate.year+2,8,self.BenchmarkDate.day) #第二季營收在8月14以前公佈
#                Q2StartDate = Q2EndDate + relativedelta(months =- self.RevenueAeverageGrowthRateMonth_Benchmark)
#            elif self.BenchmarkDate.month == 7:
#                Q2EndDate = dt.date(self.BenchmarkDate.year+2,11,self.BenchmarkDate.day) #第三季營收在11月14以前公佈
#                Q2StartDate = Q2EndDate + relativedelta(months =- self.RevenueAeverageGrowthRateMonth_Benchmark)
            
            MonthlyRevenueGrowth = MonthlyRevenueTable[[MonthlyRevenueTable.index[i] >= Q2StartDate  for i in range(0,len(MonthlyRevenueTable.index))]]
            MonthlyRevenueGrowth = MonthlyRevenueGrowth[[MonthlyRevenueGrowth.index[i] <= Q2EndDate  for i in range(0,len(MonthlyRevenueGrowth.index))]]
            
            RevenueAverageGrowth = MonthlyRevenueGrowth['YoYGrowth'].mean()
            self.InformationDataFrame.loc['RevenueAverageGrowth',str(ID)] = RevenueAverageGrowth

                
            if len(RevenueYoYGrowth) <self.MinMonthsNum or all(pd.isnull(RevenueYoYGrowth)): #少於ㄧ年的營收YoY為False
                print(str(ID)+'\t'+'RevenueYoYGrowth is less than 10 Months')
                continue
            
            if (sum(RevenueYoYGrowth > self.RevenueYoYGrowthRate_Benchmark) >= self.MinMonthsNum - self.RevenueYoYIsLessThanZeroNum_Benchmark) or (head - tail > self.RevenueAntiRate):
                self.SignalDataFrame.loc['RevenueYoYIsLessThanZero',str(ID)] = True
            else:
                self.SignalDataFrame.loc['RevenueYoYIsLessThanZero',str(ID)] = False
                            
        MonthlyRevenueConnection.close()
        print('Monthly RevenueTable is Done!!!!!')
        
        
    def CashFlowSignal(self):
        
        
        '''
        Statement Of Cash Flow 
        Q1 2 Year Cumulative Cash Flow is Positive?
        '''
        
        CashFlowConnection = pymysql.connect(
                        host='127.0.0.1',
                        user='root',
                        passwd='PASSWORD',
                        db='cashflow',
                        charset='BIG5',
                        local_infile=True
                        )            
        
        for ID in self.StockID:
            try:
                CashFlowTable = sql.read_sql('select * from '+ '`'+str(ID)+'new'+'`' +';',con= CashFlowConnection)
                CashFlowTable = CashFlowTable.drop_duplicates()
            except sql.DatabaseError:
                print(str(ID)+'\t'+'do not exist in CashFlow Database')
                continue
            if len(CashFlowTable ) <=3: #資料不存在
                print(str(ID)+'\t'+'CashFlowTable DataFrame is empty')
                continue                    
            CashFlowTable = CashFlowTable.set_index('index').sort_index(ascending = False)
            CashFlowTable.index = [dt.datetime.strptime(QuarterToMonth(CashFlowTable.index)[i],'%Y%m%d').date() for i in range(0,len(CashFlowTable.index))]
            
            ###Q1 2 Year Cumulative Cash Flow is Positive?
            Q1EndDate = dt.date(self.BenchmarkDate.year,self.BenchmarkDate.month,self.BenchmarkDate.day) - relativedelta(months = 4 ,days=14)#看兩年的營益率資料
            Q1StartDate = Q1EndDate - relativedelta(months=23)
            
            CumulativeFreeCash = CashFlowTable[[CashFlowTable.index[i] > Q1StartDate  for i in range(0,len(CashFlowTable.index))]]
            CumulativeFreeCash = CumulativeFreeCash[[CumulativeFreeCash.index[i] <= Q1EndDate  for i in range(0,len(CumulativeFreeCash.index))]]
            
            try:
                CumulativeFreeCash = CumulativeFreeCash['本期產生現金流量']
            except KeyError:
                print(str(ID)+'\t'+'CumulativeFreeCash has KeyError')
                continue
                
            if len(CumulativeFreeCash) <8 or any(x is None for x in CumulativeFreeCash.tolist()): #少於兩年的自由現金流量為False
                print(str(ID)+'\t'+'CumulativeFreeCash is less than 8 Quarters')
                continue
            else:
                CumulativeFreeCash_sum = sum(CumulativeFreeCash)
            
            if CumulativeFreeCash_sum > 0: #近兩年累計自由現金流量需為正
                self.SignalDataFrame.loc['CumulativeFreeCashIsPositive',str(ID)] = True
            else:
                self.SignalDataFrame.loc['CumulativeFreeCashIsPositive',str(ID)] = False
                
        CashFlowConnection.close()
        print('CashFlowTable is Done!!!!!')
        
        
    def CompanyInformationSignal(self):
        
        '''
        Signal:
            1. Listing Date
            
        '''
                                 
        '''
        Performance of Enterprise
        Q1 what is the average EPS in 3 Years?
        
        Company Information (Have two responses 1.CompanyInformation 2.HistoryPERatio)
        Q1 PE Ratio  
        Q2 Industry PE
        Q3 Yield Rate
        Q4 Market Value
        Q5 PB ratio
        Q6 Beta 
        Q7 SD
        Q8 Listing Date
        Q9 ShareCapital
        Q10 Return On Assets
        Q11 Shareholders' Return On Equity
        Q12 EPS rate of growth in 3 years?
        Q13 History EPS Max & Min in 5 Years
        Q14 CAPE
        Q15 CAPE/G
        '''    
        
        
        
        for ID in self.StockID:
            if ID in ['3662','4415','3219','4413']: #被暫停交易
                continue
            
            ###Q1 what is the average EPS in 3 Years?
            PerformanceConnection = pymysql.connect(
                        host='127.0.0.1',
                        user='root',
                        passwd='PASSWORD',
                        db='performance',
                        charset='BIG5',
                        local_infile=True
                        )                    
        
            try:
                PerformanceTable = sql.read_sql('select * from '+ '`'+str(ID)+'new'+'`' +';',con= PerformanceConnection)
                PerformanceTable = PerformanceTable.drop_duplicates()
            except sql.DatabaseError:
                print(str(ID)+'\t'+'do not exist in Performance Database')
                continue
            if len(PerformanceTable) <=3: #資料不存在
                print(str(ID)+'\t'+'Performance DataFrame is empty')
                continue                    
            PerformanceTable = PerformanceTable.set_index('Date').sort_index(ascending = False)
            PerformanceTable.index = [dt.datetime.strptime(QuarterToMonth(PerformanceTable.index)[i],'%Y%m%d').date() for i in range(0,len(PerformanceTable.index))]
            
            Q1EndDate = dt.date(self.BenchmarkDate.year,self.BenchmarkDate.month,self.BenchmarkDate.day) - relativedelta(months = 4 ,days=14)#看兩年的營益率資料
            Q1StartDate = Q1EndDate - relativedelta(months=35)
            
            AverageEPSIn3 = PerformanceTable[[PerformanceTable.index[i] >= Q1StartDate  for i in range(0,len(PerformanceTable.index))]]
            AverageEPSIn3 = AverageEPSIn3[[AverageEPSIn3.index[i] <= Q1EndDate  for i in range(0,len(AverageEPSIn3.index))]]
            
            try:
                AverageEPSIn3 = AverageEPSIn3['EPS']
            except KeyError:
                print(str(ID)+'\t'+'AverageEPSIn3 has KeyError')
                continue
                
            if len(AverageEPSIn3) <12 : #少於三年的EPS為False
                print(str(ID)+'\t'+'AverageEPSIn3 is less than 12 Quarters')
                continue
            else:
                AverageEPSIn3 = sum(AverageEPSIn3)/3
            
            self.InformationDataFrame.loc['AverageEPSIn3Years',str(ID)] = AverageEPSIn3
                
#            ###Q12 EPS rate of growth in 3 years(near 12 Quarters)? 
#            if self.BenchmarkDate.month == 1:
#                Q12EndDate = dt.date(self.BenchmarkDate.year+2,1,1) #看三年的EPS成長率      
#                Q12StartDate = dt.date(self.BenchmarkDate.year-1,4,1) #看三年的EPS成長率      
#            elif self.BenchmarkDate.month == 4:
#                Q12EndDate = dt.date(self.BenchmarkDate.year+2,4,1) #看三年的EPS成長率      
#                Q12StartDate = dt.date(self.BenchmarkDate.year-1,7,1) #看三年的EPS成長率    
#            elif self.BenchmarkDate.month == 7:
#                Q12EndDate = dt.date(self.BenchmarkDate.year+2,7,1) #看三年的EPS成長率      
#                Q12StartDate = dt.date(self.BenchmarkDate.year-1,10,1) #看三年的EPS成長率    
#            else:
#                Q12EndDate = dt.date(self.BenchmarkDate.year+2,10,1) #看三年的EPS成長率      
#                Q12StartDate = dt.date(self.BenchmarkDate.year,1,1) #看三年的EPS成長率    
            Q2EndDate = dt.date(self.BenchmarkDate.year,self.BenchmarkDate.month,self.BenchmarkDate.day) - relativedelta(months = 4 ,days=14)#看兩年的營益率資料
            Q2StartDate = Q2EndDate - relativedelta(months=35)

            EPSIn3Years = PerformanceTable[[PerformanceTable.index[i] >= Q2StartDate  for i in range(0,len(PerformanceTable.index))]]                            
            EPSIn3Years = EPSIn3Years[[EPSIn3Years.index[i] <= Q2EndDate  for i in range(0,len(EPSIn3Years.index))]]                                            
            
            if len(EPSIn3Years)<12:
                print(str(ID)+'\t'+'EPSIn3Years is less than 12 Quarters')
                continue
            else:
                try:
                    EPSRateOfGrowth = (((EPSIn3Years['EPS'][0:4]).sum()/(EPSIn3Years['EPS'][4:8]).sum())*((EPSIn3Years['EPS'][4:8]).sum()/(EPSIn3Years['EPS'][8:12]).sum()))**(0.5) -1 #近三年EPS年複合成長率
                    self.InformationDataFrame.loc['EPSRateOfGrowthIn3',str(ID)] = EPSRateOfGrowth
                except ZeroDivisionError:
                    self.InformationDataFrame.loc['EPSRateOfGrowthIn3',str(ID)] = np.nan
                    print(str(ID)+'\t'+'EPSRateOfGrowthIn3 has ZeroDivisionError')
                                        
            ###Company Information                            
            CompanyInformationConnection = pymysql.connect(
                                host='127.0.0.1',
                                user='root',
                                passwd='PASSWORD',
                                db='companyinformation',
                                charset='BIG5',
                                local_infile=True
                                )            
            
            try:
                CompanyInformationTable = sql.read_sql('select * from '+ '`'+str(ID)+'new'+'`' +';',con= CompanyInformationConnection)
                CompanyInformationTable = CompanyInformationTable.drop_duplicates()
            except sql.DatabaseError:
                print(str(ID)+'\t'+'do not exist in CompanyInformation Database')
                continue
            if len(CompanyInformationTable.columns) <=3: #資料不存在
                print(str(ID)+'\t'+'CompanyInformationTable DataFrame is empty')
                continue                    
            CompanyInformationTable = CompanyInformationTable.set_index('index').sort_index(ascending = False)
            CompanyInformationTable.index = [dt.datetime.strptime(CompanyInformationTable.index[0],'%Y%m%d')]
            
            #暫時因資料不足而使用
            EndDate = dt.date(self.BenchmarkDate.year,self.BenchmarkDate.month,self.BenchmarkDate.day) + relativedelta(months = 10) #10沒意義  因資料不足而設的
            StartDate = dt.date(self.BenchmarkDate.year,self.BenchmarkDate.month,self.BenchmarkDate.day) - relativedelta(months = 2)
            
            #之後要改用下面這個(當資料每季更新時)
#            EndDate = dt.date(self.BenchmarkDate.year,self.BenchmarkDate.month,self.BenchmarkDate.day) + relativedelta(months = 1 ,days=14)#看兩年的營益率資料
#            StartDate = EndDate - relativedelta(months = 2)

#            if self.BenchmarkDate.month == 10: 
#                StartDate = dt.date(self.BenchmarkDate.year+3,3,31)
#                EndDate = dt.date(self.BenchmarkDate.year+3,5,14) #第四季營收在隔年3月31以前公佈
#            elif self.BenchmarkDate.month == 1:
#                StartDate = dt.date(self.BenchmarkDate.year+2,5,15)
#                EndDate = dt.date(self.BenchmarkDate.year+2,8,13) #第一季營收在5月15以前公佈
#            elif self.BenchmarkDate.month == 4:
#                StartDate = dt.date(self.BenchmarkDate.year+2,8,14)
#                EndDate = dt.date(self.BenchmarkDate.year+2,11,13) #第二季營收在8月14以前公佈
#            elif self.BenchmarkDate.month == 7:
#                StartDate = dt.date(self.BenchmarkDate.year+2,11,14)
#                EndDate = dt.date(self.BenchmarkDate.year+3,3,30) #第三季營收在11月14以前公佈
                
            CompanyInformationTable = CompanyInformationTable[[CompanyInformationTable.index[i].date() >= StartDate  for i in range(0,len(CompanyInformationTable.index))]]                            
            CompanyInformationTable = CompanyInformationTable[[CompanyInformationTable.index[i].date() <= EndDate  for i in range(0,len(CompanyInformationTable.index))]]                                            
            
            self.InformationDataFrame.loc['PE',str(ID)] = CompanyInformationTable.PE[0]
            self.InformationDataFrame.loc['IndustryPE',str(ID)] = CompanyInformationTable.IndustryPE[0]
            self.InformationDataFrame.loc['YieldRate',str(ID)] = CompanyInformationTable.YieldRate[0]
            self.InformationDataFrame.loc['MarketValue',str(ID)] = CompanyInformationTable.MarketValue[0]
            self.InformationDataFrame.loc['PB',str(ID)] = CompanyInformationTable.PB[0]
            self.InformationDataFrame.loc['Beta',str(ID)] = CompanyInformationTable.Beta[0]
            self.InformationDataFrame.loc['SD',str(ID)] = CompanyInformationTable.SD[0]
            self.InformationDataFrame.loc['ShareCapital',str(ID)] = CompanyInformationTable.ShareCapital[0]
            self.InformationDataFrame.loc['ClosePrice',str(ID)] = CompanyInformationTable.ClosePrice[0]
            self.InformationDataFrame.loc['ROE',str(ID)] = CompanyInformationTable.ROE[0]
            self.InformationDataFrame.loc['ROA',str(ID)] = CompanyInformationTable.ROE[0]
            
            self.InformationDataFrame.loc['ListingDate',str(ID)] = CompanyInformationTable.ListingDate[0] #剔除成立低於5年的公司
            
            CompanyInformationConnection.close()
                                            
    
            ###StockPriceTable
            StockPriceTableConnection = pymysql.connect(
                host='127.0.0.1',
                user='root',
                passwd='PASSWORD',
                db='stockprice',
                charset='BIG5',
                local_infile=True
                )            
            try:
                StockPriceTable = sql.read_sql('select * from '+ '`'+str(ID)+'new'+'`' +';',con= StockPriceTableConnection)
                StockPriceTable = StockPriceTable.drop_duplicates()
            except sql.DatabaseError:
                print(str(ID)+'\t'+'do not exist in StockPriceTable Database')
                continue
            if len(StockPriceTable) <=3: #資料不存在
                print(str(ID)+'\t'+'StockPriceTable DataFrame is empty')
                continue                    
            StockPriceTable = StockPriceTable.sort_values('Date',ascending=False)
            StockPriceTable = StockPriceTable.set_index('Date')
            Strptime = list(map(str,[StockPriceTable.index[i] for i in range(0,len(StockPriceTable.index))]))
            Date = [dt.datetime.strptime(Strptime[i][0:10],'%Y-%m-%d').date() for i in range(0,len(Strptime))]
            StockPriceTable.index = Date
            StockPriceTable = StockPriceTable.apply(pd.to_numeric, errors='coerce')
            
            EndDate = dt.date(self.BenchmarkDate.year,self.BenchmarkDate.month,self.BenchmarkDate.day) + relativedelta(days = 10)
            StartDate = dt.date(self.BenchmarkDate.year,self.BenchmarkDate.month,self.BenchmarkDate.day) - relativedelta(days = 10)

            
#            if self.BenchmarkDate.month == 1:
#                StartDate = dt.date(self.BenchmarkDate.year+2,self.BenchmarkDate.month+4,15) 
#                EndDate = dt.date(int(t[0:4]),int(t[4:6]),int(t[6:8])) 
#            elif self.BenchmarkDate.month == 4:
#                StartDate = dt.date(self.BenchmarkDate.year+2,self.BenchmarkDate.month+4,14) 
#                EndDate = dt.date(int(t[0:4]),int(t[4:6]),int(t[6:8])) 
#            elif self.BenchmarkDate.month == 7:
#                StartDate = dt.date(self.BenchmarkDate.year+2,self.BenchmarkDate.month+4,14) 
#                EndDate = dt.date(int(t[0:4]),int(t[4:6]),int(t[6:8])) 
#            elif self.BenchmarkDate.month == 10:
#                StartDate = dt.date(self.BenchmarkDate.year+3,self.BenchmarkDate.month-7,10) 
#                EndDate = dt.date(int(t[0:4]),int(t[4:6]),int(t[6:8])) 
                             
            StockPrice = StockPriceTable[[StockPriceTable.index[i] >= StartDate  for i in range(0,len(StockPriceTable.index))]]
            StockPrice = StockPrice[[StockPrice.index[i] <= EndDate  for i in range(0,len(StockPrice.index))]]
            
            if len(StockPrice) == 0 :
                print(str(ID)+'\tStockPrice is too short')
                continue
            #Cumulative Rutrun
#            if self.BenchmarkDate.month == 1:
#                StartDate = dt.date(self.BenchmarkDate.year+2,self.BenchmarkDate.month+4,15) 
#                EndDate = dt.date(self.BenchmarkDate.year+2,self.BenchmarkDate.month+7,13) 
#            elif self.BenchmarkDate.month == 4:
#                StartDate = dt.date(self.BenchmarkDate.year+2,self.BenchmarkDate.month+4,14) 
#                EndDate = dt.date(self.BenchmarkDate.year+2,self.BenchmarkDate.month+8,13)
#            elif self.BenchmarkDate.month == 7:
#                StartDate = dt.date(self.BenchmarkDate.year+2,self.BenchmarkDate.month+4,14) 
#                EndDate = dt.date(self.BenchmarkDate.year+3,self.BenchmarkDate.month-4,30)
#            elif self.BenchmarkDate.month == 10:
#                StartDate = dt.date(self.BenchmarkDate.year+3,self.BenchmarkDate.month-7,31) 
#                EndDate = dt.date(self.BenchmarkDate.year+3,self.BenchmarkDate.month-5,14)
            
            EndDate = dt.date(self.BenchmarkDate.year,self.BenchmarkDate.month,self.BenchmarkDate.day) + relativedelta(days = 15)
            StartDate = dt.date(self.BenchmarkDate.year,self.BenchmarkDate.month,self.BenchmarkDate.day) - relativedelta(months = 3)

            
            CumulativeReturn = StockPriceTable[[StockPriceTable.index[i] >= StartDate  for i in range(0,len(StockPriceTable.index))]]
            CumulativeReturn = CumulativeReturn[[CumulativeReturn.index[i] <= EndDate  for i in range(0,len(CumulativeReturn.index))]]
            CumulativeReturn = (CumulativeReturn['AdjClose'].head(1)[0] - CumulativeReturn['AdjClose'].tail(1)[0]) / CumulativeReturn['AdjClose'].tail(1)[0] *100            
            #CumulativeReturn
            self.InformationDataFrame.loc['CumulativeReturn',str(ID)] = float(CumulativeReturn)
            #AverageStockPriceIn10Days
            AverageStockPriceIn10Days = np.nanmean(StockPrice['AdjClose'])
            
            #CAPE
            self.InformationDataFrame.loc['CAPE',str(ID)] = float(AverageStockPriceIn10Days)/float(self.InformationDataFrame.loc['AverageEPSIn3Years',str(ID)]) #價格除以近三年平均EPS
            try:
                self.InformationDataFrame.loc['CAPE_DividendBy_G',str(ID)] = self.InformationDataFrame.loc['CAPE',str(ID)]/(self.InformationDataFrame.loc['EPSRateOfGrowthIn3',str(ID)]*100) #PE Ratio 除以 G(EPS 年複合成長率) ，小於0.5代表便宜 等於0.5代表正常，大於0.5表示太貴
            except ZeroDivisionError:
                self.InformationDataFrame.loc['CAPE_DividendBy_G',str(ID)] = np.nan
                print(str(ID)+'\t'+'CAPE_DividendBy_G has ZeroDivisionError')
                                    
            ###Listing in Market over 5 years?
                                    
            if self.InformationDataFrame.loc['ListingDate',str(ID)].date() < dt.date(self.BenchmarkDate.year,self.BenchmarkDate.month,self.BenchmarkDate.day)  + relativedelta(years=-2):
                self.SignalDataFrame.loc['ListingMoreThan5Years',str(ID)] = True
            else:
                self.SignalDataFrame.loc['ListingMoreThan5Years',str(ID)] = False
            
            ###
            if float(self.InformationDataFrame.loc['MarketValue',str(ID)]) > self.MarketValue*100:
                self.SignalDataFrame.loc['MarketValue',str(ID)] = True
            else:
                self.SignalDataFrame.loc['MarketValue',str(ID)] = False
            
            ###History EPS Max & Min in 5 Years
            PEHistoryTableConnection = pymysql.connect(
                            host='127.0.0.1',
                            user='root',
                            passwd='PASSWORD',
                            db='pehistory',
                            charset='BIG5',
                            local_infile=True
                            )            
        
            try:
                PEHistoryTable = sql.read_sql('select * from '+ '`'+str(ID)+'new'+'`' +';',con= PEHistoryTableConnection)
                PEHistoryTable = PEHistoryTable.drop_duplicates()
            except sql.DatabaseError:
                print(str(ID)+'\t'+'do not exist in PEHistoryTable Database')
                continue
            if len(PEHistoryTable) <=3: #資料不存在
                print(str(ID)+'\t'+'PEHistoryTable DataFrame is empty')
                continue                    
            PEHistoryTable = PEHistoryTable.set_index('Year')
            PEHistoryTable.index = [str(int(list(PEHistoryTable.index)[i])+1911) for i in range(0,len(PEHistoryTable.index))]
            PEHistoryTable.index = [dt.datetime.strptime(PEHistoryTable.index[i],'%Y').date() for i in range(0,len(PEHistoryTable.index))]
            
            EndDate = dt.date(self.BenchmarkDate.year,self.BenchmarkDate.month,self.BenchmarkDate.day) + relativedelta(years = 1)
            StartDate = dt.date(self.BenchmarkDate.year,self.BenchmarkDate.month,self.BenchmarkDate.day) - relativedelta(years = 6)
            
            PEHistory = PEHistoryTable[[PEHistoryTable.index[i] >= StartDate  for i in range(0,len(PEHistoryTable.index))]]
            PEHistory = PEHistory[[PEHistory.index[i] <= EndDate  for i in range(0,len(PEHistory.index))]]
            
            if PEHistory['MaxPE'].apply(math.isnan).sum()>2:
                print(str(ID)+'\t'+'PEHistory do not have 4 years Data')
                continue
            
            MaxPE = PEHistory['MaxPE'].max()
            MinPE = PEHistory['MinPE'].min()
                
            self.InformationDataFrame.loc['MaxPE',str(ID)] = MaxPE
            self.InformationDataFrame.loc['MinPE',str(ID)] = MinPE
            self.InformationDataFrame.loc['CAPEMinusMaxPE',str(ID)] = self.InformationDataFrame.loc['CAPE',str(ID)] - MaxPE
            self.InformationDataFrame.loc['CAPEMinusMinPE',str(ID)] = self.InformationDataFrame.loc['CAPE',str(ID)] - MinPE
            
            ###Calculate CAPE Divided by RevenueAverageGrowth
            try:
                self.InformationDataFrame.loc['CAPEDividedRevenueAverageGrowth',str(ID)] = self.InformationDataFrame.loc['CAPE',str(ID)]/(self.InformationDataFrame.loc['RevenueAverageGrowth',str(ID)]*100) #PE Ratio 除以 G(EPS 年複合成長率) ，小於0.5代表便宜 等於0.5代表正常，大於0.5表示太貴
            except ZeroDivisionError:
                self.InformationDataFrame.loc['CAPEDividedRevenueAverageGrowth',str(ID)] = np.nan
                print(str(ID)+'\t'+'CAPEDividedRevenueAverageGrowth has ZeroDivisionError')
            
            
            
        print('CompanyinformationTable is Done!!!!!')    
            
            
    def CAPEDividedGSignal(self,CAPEDividedG_Benchmark = 0.5):
        self.CAPEDividedG_Benchmark = CAPEDividedG_Benchmark
        
        for ID in self.StockID:
             if type(self.InformationDataFrame.loc['CAPE_DividendBy_G',str(ID)]) is complex:
                print(str(ID)+'\t'+'Information\'s CAPE_DividendBy_G is NaN')
                continue
             if math.isnan(self.InformationDataFrame.loc['CAPE_DividendBy_G',str(ID)]):
                print(str(ID)+'\t'+'Information\'s CAPE_DividendBy_G is NaN')
                continue
             else:
                try:
                    if self.InformationDataFrame.loc['CAPE_DividendBy_G',str(ID)] < self.CAPEDividedG_Benchmark and self.InformationDataFrame.loc['CAPE',str(ID)]>0 and self.InformationDataFrame.loc['CAPE_DividendBy_G',str(ID)] > 0:
                        self.SignalDataFrame.loc['CAPE_DividendBy_GIsLessThanBenchmark',str(ID)] = True
                    else:
                        self.SignalDataFrame.loc['CAPE_DividendBy_GIsLessThanBenchmark',str(ID)] = False
                except TypeError:
                    print(str(ID)+'\t'+'Information\'s CAPE_DividendBy_G is None')
                    continue
        print('CAPEDividedGSignal is Done!!!')
                
        
    def CAPEMinusMinPESignal(self,CAPEMinusMinPE_Benchmark = 0):
        self.CAPEMinusMinPE_Benchmark = CAPEMinusMinPE_Benchmark
        
        for ID in self.StockID:
             if math.isnan(self.InformationDataFrame.loc['CAPEMinusMinPE',str(ID)]):
                print(str(ID)+'\t'+'Information\'s CAPEMinusMinPE is NaN')
                continue
             else:
                try:
                    if self.InformationDataFrame.loc['CAPEMinusMinPE',str(ID)] < self.CAPEMinusMinPE_Benchmark and self.InformationDataFrame.loc['CAPE',str(ID)]>0  :
                        self.SignalDataFrame.loc['CAPEMinusMinPE',str(ID)] = True
                    else:
                        self.SignalDataFrame.loc['CAPEMinusMinPE',str(ID)] = False
                except TypeError:
                    print(str(ID)+'\t'+'Information\'s CAPE_DividendBy_G is None')
                    continue
        print('CAPEDividedGSignal is Done!!!')
        
    def CAPEDividedRevenueAverageGrowthSignal(self,CAPEDividedRevenueAverageGrowth_Benchmark = 0.4):
        self.CAPEDividedRevenueAverageGrowth_Benchmark = CAPEDividedRevenueAverageGrowth_Benchmark
        
        for ID in self.StockID:
             if math.isnan(self.InformationDataFrame.loc['CAPEDividedRevenueAverageGrowth',str(ID)]):
                 print(str(ID) +'\t'+'Information\'s CAPEDividedRevenueAverageGrowth is NaN')
                 continue
             else:
                try:
                    if self.InformationDataFrame.loc['CAPEDividedRevenueAverageGrowth',str(ID)] < self.CAPEDividedRevenueAverageGrowth_Benchmark and self.InformationDataFrame.loc['CAPE',str(ID)]>0 and self.InformationDataFrame.loc['CAPEDividedRevenueAverageGrowth',str(ID)] > 0:
                        self.SignalDataFrame.loc['CAPE_DividendBy_RevenueAverageGrowthIsLessThanBenchmark',str(ID)] = True
                    else:
                        self.SignalDataFrame.loc['CAPE_DividendBy_RevenueAverageGrowthIsLessThanBenchmark',str(ID)] = False
                except TypeError:
                    print(str(ID)+'\t'+'Information\'s CAPEDividedRevenueAverageGrowth is None')
                    continue
        print('CAPEDividedRevenueAverageGrowthSignal is Done!!!')
        
        
    def __str__(self,N):
        self.N = N
        Select = self.SignalDataFrame.apply(axis=0,func=sum)
        return(self.InformationDataFrame.loc[:,Select[Select>=self.N].index])
        
        
        
        
        
        
        
        
        
        
        
        