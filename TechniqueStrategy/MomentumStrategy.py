# -*- coding: utf-8 -*-
"""
Created on Wed Jan 11 05:26:36 2017

@author: SUNG ANN LEE
"""

import pandas as pd
import pandas.io.data as web
import datetime as dt
import numpy as np
import matplotlib.pyplot as plt


def RawData(ID , Start = '2011-1-1', End = '2016-12-31'):
    
    S = dt.datetime(int(Start.split('-')[0]),int(Start.split('-')[1]), int(Start.split('-')[2]))
    E = dt.datetime(int(End.split('-')[0]),int(End.split('-')[1]), int(End.split('-')[2]) )
    
    df = web.DataReader('%s.tw'% (ID), 'yahoo' , S, E)
    
    return(df)
    

def TradingStrategy(ID, N):
    
    global RawReturn , RawDat
    
    RawDat = RawData(ID, Start = '2011-1-1', End = '2017-3-27')
    Data = RawDat
    
    Return = Data['Adj Close'].pct_change(1)[1:]
    RawReturn = Return
    
    Temp = pd.rolling_sum(np.where(Return>=0 , 1, -1),N)
    Indicator = pd.Series(Temp, index = Return.index).shift(1)
    #Indicator = pd.Series(temp, index = Return.index) 
    
    Data = pd.DataFrame({
                           'Return':Return,
                           'Indicator':Indicator
                          })
    
    Temp_Signal = np.where(Indicator > 0 , 1, np.where(Indicator < 0 , -1 , 2))
    Temp_Signal = pd.DataFrame({
                                'Signal':Temp_Signal
                                })
    
    #大於0為1 小於0為-1 等於零為2
    Signal = Temp_Signal.diff()
    #-1買進 -3賣出
    Data['Signal'] = Signal.values
    
    
    LongTime = list()
    ShortTime = list()
    
    for i in range(0,Data.shape[0]):
        if Data['Signal'][i] == -1:
            LongTime.append(Data.index[i])
        if Data['Signal'][i] == -3:
            ShortTime.append(Data.index[i])
    
    Time = pd.DataFrame({
                         'Time':LongTime,
                         'Sig':np.repeat(-1,len(LongTime))
                         })
    Time = Time.append(pd.DataFrame({
                                      'Time':ShortTime,
                                      'Sig':np.repeat(-3,len(ShortTime))
                                      }))
    
    TradingTime = Time.sort_values('Time')
    TradingTime['Sig'] = TradingTime['Sig'].diff()
    TradingTime.index = range(0,len(TradingTime))
    TradingTime = TradingTime[TradingTime['Sig'] != 0][1:]
    TradingTime.index = range(0,len(TradingTime))
    
    if TradingTime['Sig'].head(1).values == -2:
        TradingTime = TradingTime[1:len(TradingTime)]
    
    if TradingTime['Sig'].tail(1).values == 2:
        TradingTime = TradingTime[0:len(TradingTime)-1]
    
    global PerTrade                                  
                                  
    Buy = list(TradingTime[TradingTime['Sig'] == 2]['Time'])
    Sell = list(TradingTime[TradingTime['Sig'] == -2]['Time'])
    PerTrade = pd.DataFrame({
                             'BuyTime':Buy,
                             'SellTime':Sell
                             })
    PerTrade['PeriodReturn'] = float()
    
    for i in range(0,PerTrade.shape[0]):
        PeriodData = Data.loc[PerTrade['BuyTime'][i]:PerTrade['SellTime'][i]] 
        PerTrade.set_value(i,'PeriodReturn', np.prod(PeriodData['Return'] + np.array(1)) - 1)

    #Outcome
        
    TradingTimes = len(PerTrade['PeriodReturn'])
    PerTradeDays = PerTrade['SellTime'] - PerTrade['BuyTime']     
    AvgTradeDays = np.mean(PerTradeDays)

    WinDays =  sum(PerTrade['PeriodReturn']  > 0)
    WinRate = WinDays / TradingTimes
    
    AvgWin = np.mean(PerTrade['PeriodReturn'][PerTrade['PeriodReturn'] > 0])
    AvgLoss  = np.mean(PerTrade['PeriodReturn'][PerTrade['PeriodReturn'] < 0])      
    
    TotalWinReturn = sum(PerTrade['PeriodReturn'][PerTrade['PeriodReturn'] > 0])    
    TotalLossReturn = sum(PerTrade['PeriodReturn'][PerTrade['PeriodReturn'] < 0])        
    
    TotalReturn = np.prod(PerTrade['PeriodReturn'] +np.array(1) )-1
    ProfitFactor = TotalWinReturn / -TotalLossReturn
    
        
    print('\n',
          'Stock Index: ', ID, '\n',
          'TradingTimes: ', TradingTimes, '\n',
          'AvgTradeDays: ', AvgTradeDays.days, 'Days', '\n',
          'WinDays: ', WinDays, 'Days','\n',
          'WindRate: ', WinRate, '%','\n',
          'AvgWin: ', round(100*AvgWin,4), '%','\n',
          'AvgLoss: ' , round(100*AvgLoss,4), '%','\n',
          'TotalReturn: ',round(100*TotalReturn,4), '%','\n',
          'ProfitFactor: ',ProfitFactor)    
        
       
            
       
    Values = np.cumsum(PerTrade['PeriodReturn'])
    RawPrice = RawDat['Adj Close']
    BuyAndHoldReutrn = RawPrice.loc[PerTrade['SellTime']] / np.array(RawPrice[0]) - 1
    Timestamps = PerTrade['SellTime']
    
    plt.plot(Timestamps,Values, label='Momentum')
    plt.plot(Timestamps,BuyAndHoldReutrn, label = 'BuyAndHold')
    plt.title('Stock'+str(ID))
    plt.legend(loc=2)
    plt.show()  

    
    














