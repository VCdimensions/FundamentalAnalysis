# -*- coding: utf-8 -*-
"""
Created on Fri Feb 24 01:55:37 2017

@author: SUNG ANN LEE
"""
'''
跳過股票
6538 (這支股票日期資料有問題)
3662 (被暫停交易)
4415 (被暫停交易)
'''
import FundamentalAnalysis.FundamentalStrategy.LongTermValueStrategy as SelectStock
#import FundamentalAnalysis.TechniqueStrategy.MomentumStrategy as Momentum
#import datetime as dt

'''
更新資料庫
'''
UpdateData = SelectStock.LongTermValueStrategy(BenchmarkDate='20180815') #資料庫更新為添加新資料 不是整張表更新
UpdateData.UpdateQuarteryData() 
UpdateData.UpdateMonthyData()
UpdateData.UpdateCompanyInfo()
UpdateData.UpdateStockPrice()
 

'''
Signal
'''

MatchSignalStock = SelectStock.LongTermValueStrategy(BenchmarkDate='20180815', #0331表示去年第一季出來，0515表示今年第一季出來，0815表示今年第二季出來，1115表示今年第三季出來
                                                OperatingProfitRatio_Std_Benchmark = 0.03, #營業利益率近8季標準差小於5%
                                                InventoryTurnover_Std_Benchmark = 0.25, #存貨周轉率近8季標準差小於30%
                                                RevenueYoYIsLessThanZeroNum_Benchmark = 3, #營收年增率近12個月最多只能有RevenueYoYIsLessThanZeroNum_Benchmark個月小於RevenueYoYGrowthRate_Benchmark
                                                RevenueYoYGrowthRate_Benchmark = 0, #營收年增率近12個月最多只能有RevenueYoYIsLessThanZeroNum_Benchmark個月小於RevenueYoYGrowthRate_Benchmark
                                                RevenueAeverageGrowthRateMonth_Benchmark = 13, #用近12個月的營收年增率來計算營收的平均年成長率
                                                RevenueAntiRate = .2, #累積年增率一整年的差值，越大表示近一年的累積營收有往上的趨勢
                                                MinMonthsNum = 12, #營收回測有幾個月
                                                MarketValue = 50) #市值超過50億的公司
'''
符合所有條件的股票
'''
MatchSignalStock.ProfitablilitySignal() #1.近八季營業利益率穩定 2. 近4年NI為正
MatchSignalStock.FinancialRatioSignal() #近八季存貨周轉率穩定

#MatchSignalStock.MinMonthsNum = 10 #回測近10個月的營收(目前是20170302三月營收還沒出來 而財報才公布到2015第三季 所以只有營收的時間要特別調整
#MatchSignalStock.BenchmarkDate = dt.datetime.strptime('20141001','%Y%m%d').date()    #輸入20141001會看20160401到20170301的營收資料
MatchSignalStock.MonthlyRevenueSignal() #近12個月的營收年增率為正

#MatchSignalStock.BenchmarkDate = dt.datetime.strptime('20141001','%Y%m%d').date()
MatchSignalStock.CashFlowSignal() #近8季現金為流入
MatchSignalStock.CompanyInformationSignal() #近五年上市的股票不要(執行這個一定要先執行MonthlyRevenueSignal,因為要算RevenueAverageGrowth)

MatchSignalStock.CAPEDividedGSignal(CAPEDividedG_Benchmark=4.7) #CAPE除以盈餘年複合成長率小於1
#MatchSignalStock.CAPEMinusMinPESignal(CAPEMinusMinPE_Benchmark=8) #CAPE減去近五年最低EPS小於零
#MatchSignalStock.CAPEDividedRevenueAverageGrowthSignal(CAPEDividedRevenueAverageGrowth_Benchmark = 0.5) #CAPE除以營收平均年成長率小於0.4

Signal = MatchSignalStock.SignalDataFrame 
Information = MatchSignalStock.InformationDataFrame

MatchConditionStock = MatchSignalStock.__str__(N=len(Signal)) #全部設定條件都符合的股票
MatchConditionSignal = Signal.loc[:,MatchConditionStock.columns]

#MatchConditionStock.loc['CumulativeReturn',:].mean()
#Signal.iloc[2,].sum()
#(set(MatchConditionSignal.columns) & set(MatchConditionSignal2.columns)) #連續兩季都有出現的標的


#Signal.to_csv('Signal.csv')
#Information.to_csv('Information.csv')
#MatchConditionSignal.to_csv('MatchConditionSignal.csv')
#MatchConditionStock.to_csv('MatchConditionStock.csv')

'''
符合財物的自由世界一書選股原則的股票
'''                               
#MatchSignalStock.ProfitablilitySignal() #1.近八季營業利益率穩定 2. 近4年NI為正
#MatchSignalStock.FinancialRatioSignal() #近八季存貨周轉率穩定
#MatchSignalStock.MonthlyRevenueSignal() #近12個月的營收年增率為正
#MatchSignalStock.CashFlowSignal() #近8季現金為流入
#MatchSignalStock.CompanyInformationSignal() #近五年上市的股票不要
#                               
#Signal = MatchSignalStock.SignalDataFrame 
#Information = MatchSignalStock.InformationDataFrame
#
#MatchAllConditionStock = MatchSignalStock.__str__(N=len(Signal)-1) #全部設定條件都符合的股票
#
#Signal.apply(axis=0,func=sum)

'''
Reasonable Price
StkNum:表示財務報表滿足訊號的股票代號  Beta:表示一個調整係數，牛市時大於一，熊市時小於一  
'''
import FundamentalAnalysis.FundamentalStrategy.ReasonablePrice as RP

ReasonablePriceTable = RP.ReasonablePrice(StkNum = MatchConditionSignal.columns, Beta = 0.85, BenchmarkDate = '20180815')
ReasonablePriceTable = RP.ReasonablePrice(StkNum = ['3090'], Beta = 0.85, BenchmarkDate = '20180815')
ReasonablePriceTable

'''
Momentum Strategy
'''
#MatchConditionSignal.columns
#for i in MatchConditionSignal.columns:
#    for j in [12,14,16]:
#        print('N Parameter is\t'+str(j))    
#        Momentum.TradingStrategy(i,j)
#
#












                            