# -*- coding: utf-8 -*-
"""
Created on Sat Feb 11 18:42:20 2017

@author: SUNG ANN LEE
"""
import requests
from lxml import etree
import pandas as pd
import numpy as np
import datetime as dt


class FinancialStatement:

    def __init__(self,ID):
        self.ID = ID

    '''
    Crawl All Listed Stock and Delete KY- Stock
    '''
    
    def StockNumber(KYDelete=True):
        #上市公司
        Source = requests.get ('http://isin.twse.com.tw/isin/C_public.jsp?strMode=2') 
        Source.encoding = 'Big5'
        
        ##匯出html成txt檔
        #with open('Source.txt', mode='w', encoding='utf-8') as file:
        #    file.write(Source)
        
        Tree = etree.HTML(Source.text)
        Tree2 = Tree.xpath('/html//table[@class="h4"]//tr/td[1]')
        
        Stock = []
        for item in Tree2:
            Stock.append(item.text)
        
        Temp = str(Stock)
        Temp = str(Temp.split('\\u3000'))
        a = Temp.find('None') #第一個None出現
        b = Temp.find('None',18) #取到第2個None出現
        Temp = Temp[a+5:b-2].split(',')
        Stk = pd.DataFrame({
                               'StkName':Temp[1:len(Temp):2],
                               'StkNum':Temp[0:len(Temp):2]
                                             },
                                             dtype=str)
        Stk1 = Stk.replace(['"',"'"],['',''],regex=True)
        
        #上櫃公司
        Source = requests.get ('http://isin.twse.com.tw/isin/C_public.jsp?strMode=4') 
        Source.encoding = 'Big5'
        
        
        Tree = etree.HTML(Source.text)
        Tree2 = Tree.xpath("/html//table[@class='h4']//tr/td[1]")
        
        Stock = []
        for item in Tree2:
            Stock.append(item.text)
        
        a = [i for i,x in enumerate(Stock) if x == None][2] #第3個None出現
        b = [i for i,x in enumerate(Stock) if x == None][3] #第4個None出現
        Temp = Stock[a+1:b]
        
        Stk2 = pd.DataFrame({
                           'StkName':[Temp[i].split('\u3000')[1] for i in range(0,len(Temp))],
                           'StkNum':[Temp[i].split('\u3000')[0] for i in range(0,len(Temp))]
                                         },
                                         dtype=str)
        
        Stk = Stk1.append(Stk2)
        
        if KYDelete == True:
            Stk = Stk[Stk['StkName'].str.contains("KY")==False]
                      
        print('共有',Stk.shape[0],'支股票符合資格')
        return(Stk)
        
    
        
    '''
    Profitability Table
    Q1 OperatingProfitRatio is stable near 8 quarter?
    Q2 4 Year Cumulative Net Income is Positive?
    '''
    
    def ProfitabilityTable(self):
        ID = self.ID        
                
        Source = requests.get('http://jsjustweb.jihsun.com.tw/z/zc/zce/zce_%s.djhtm'%(ID)) 
        Source.encoding = 'Big5'
        
        Tree = etree.HTML(Source.text)
        Tree2 = Tree.xpath("/html//td[@class='t0']//td")
        
        Data = []
        for item in Tree2:
            Data.append(item.text)
        Data = Data[2:]
        
        if any(x is None for x in Data):
            print('\n'+str(ID)+'\n'+'Profitability Have None Data')
            Data = [np.NaN if x is None else x for x in Data]
            
        if any(x == 'N/A' for x in Data):
            print('\n'+str(ID)+'\n'+'Profitability Have NaN Data')
            Data = [np.NaN if x == 'N/A' else x for x in Data]        
            
        length = len(Data)
        ColName = ['Date','Revenue','Cost','GrossMargin','GrossRatio','OperatingMargin',
                   'OperatingProfitRatio','ExternalRevenueExpense','PreTaxIncome','NetIncome']
        
        Profitability = pd.DataFrame()           
        for i in range(10,20,1):
            CName = ColName[i-10]
            TempData = pd.Series() 
            for j in range(i,length,10):
                TempData = TempData.append(pd.Series(Data[j]))
            Profitability[CName] = TempData
        
        Profitability = Profitability.set_index('Date')
        Profitability = Profitability.replace([',','%'],['',''],regex=True)
        Profitability = Profitability.apply(pd.to_numeric)
        Profitability[['GrossRatio','OperatingProfitRatio']] = Profitability[['GrossRatio','OperatingProfitRatio']]/100
        Profitability.index = pd.Series(Profitability.index).replace(["\.",'1Q','2Q','3Q','4Q'],["","01","04","07","10"],regex=True)
        
        print('\n'+str(ID)+'\n'+'Profitability Be Done')
        
        return(Profitability)    
    
    '''
    Performance of Enterprise
    Q1 what is the average EPS near 4 Year?
    '''
    def PerformanceTable(self):
        ID = self.ID                
        
        Source = requests.get('http://jsjustweb.jihsun.com.tw/z/zc/zcd/zcd_%s.djhtm'%(ID)) 
        Source.encoding = 'Big5'
        
        Tree = etree.HTML(Source.text)
        Tree2 = Tree.xpath("/html//td[@class='t0']//td")
        
        Data = []
        for item in Tree2:
            Data.append(item.text)
        Data = Data[1:]
        
        if any(x == None for x in Data):
            print('\n'+str(ID)+'\n'+'Performance Have None Data')
            Data = [np.NaN if x is None else x for x in Data]
        
        if any(x == 'N/A' for x in Data):
            print('\n'+str(ID)+'\n'+'Performance Have NaN Data')
            Data = [np.NaN if x == 'N/A' else x for x in Data]
            
        length = len(Data)
        ColName = ['Date','WeightedAverageEquity','Income','PreTaxIncome','NetIncome','IncomePerShare',
                   'PreTaxEPS','EPS']
        
        Performance = pd.DataFrame()           
        for i in range(8,16,1):
            CName = ColName[i-8]
            TempData = pd.Series() 
            for j in range(i,length,8):
                TempData = TempData.append(pd.Series(Data[j]))
            Performance[CName] = TempData
        
        Performance = Performance.set_index('Date')
        Performance = Performance.replace([',','%'],['',''],regex=True)
        Performance = Performance.apply(pd.to_numeric)
        Performance.index = pd.Series(Performance.index).replace(["\.",'1Q','2Q','3Q','4Q'],["","01","04","07","10"],regex=True)
        
        print('\n'+str(ID)+'\n'+'Performance Be Done')
        
        return(Performance)    
    
    
    '''
    FinancialRatio
    Q1 How about the Inventory Turnover is declinging?
    '''
    def FinancialRatioTable(self):
        ID = self.ID        
        
        Source = requests.get('http://jsjustweb.jihsun.com.tw/z/zc/zcr/zcr_%s.djhtm'%(ID)) 
        Source.encoding = 'Big5'
        
        Tree = etree.HTML(Source.text)
        Tree2 = Tree.xpath("/html//td[@class='t0']//td")
        
        Data = []
        for item in Tree2:
            Data.append(item.text)
        Data = Data[1:]
        
        if len(Data) <= 2:
            FinancialRatio = pd.DataFrame()    
            return(FinancialRatio)
        
        Data.remove('獲利能力指標')
        Data.remove('每股比率指標')
        Data.remove('成長率指標')
        Data.remove('經營能力指標')
        Data.remove('償債能力指標')
        Data.remove('成本費用率指標')
        Data.remove('其他')
        if '法定比率'  in Data:
            Data.remove('法定比率')
        
        #Data.remove('獲利能力')
        #Data.remove('經營績效')
        #Data.remove('償債能力')
        #Data.remove('經營能力')
        #Data.remove('資本結構')
        
        if any(x == None for x in Data):
            print('\n'+str(ID)+'\n'+'FinancialRatio Have None Data')
            Data = [np.NaN if x is None else x for x in Data]
        
        if any(x == 'N/A' for x in Data):
            print('\n'+str(ID)+'\n'+'FinancialRatio Have NaN Data')
            Data = [np.NaN if x == 'N/A' else x for x in Data]
        
        
        length = len(Data)
        
        if length < 156:
            print('\n'+str(ID)+'\n'+'FinancialRatio is not existed')
            FinancialRatio = pd.DataFrame()    
            return(FinancialRatio)
        else:
            
            if length == 279:
                NumOfPeriod = int((length)/31)
            elif length == 248:
                NumOfPeriod = int((length)/31)
            elif length == 217:
                NumOfPeriod = int((length)/31)
            elif length == 186:
                NumOfPeriod = int((length)/31)
            elif length == 249:
                NumOfPeriod = int((length)/83)
            elif length == 155:
                NumOfPeriod = int((length)/31)
            elif length == 332:
                NumOfPeriod = int((length)/83)
            elif length == 498:
                NumOfPeriod = int((length)/83)
            elif length == 581:
                NumOfPeriod = int((length)/83)
            elif length == 648:
                NumOfPeriod = int((length)/72)
            elif length == 664:
                NumOfPeriod = int((length)/83)
            elif length == 684:
                NumOfPeriod = int((length)/76)
            elif length == 711:
                NumOfPeriod = int((length)/79)
            elif length == 747:
                NumOfPeriod = int((length)/83)
            elif length == 702:
                NumOfPeriod  = int((length)/78)
        
            ColName = Data[0:9]
                       
            FinancialRatio = pd.DataFrame()           
            for i in range(NumOfPeriod,NumOfPeriod*2,1):
                CName = ColName[i-NumOfPeriod]
                TempData = pd.Series() 
                for j in range(i,length,NumOfPeriod):
                    TempData = TempData.append(pd.Series(Data[j]))
                FinancialRatio[CName] = TempData
            
            
            FinancialRatio = FinancialRatio.set_index('期別')
            
            FinancialRatio = FinancialRatio.replace([',','%'],['',''],regex=True)
            FinancialRatio = FinancialRatio.transpose()
            FinancialRatio = FinancialRatio.drop('種類',1)
            FinancialRatio = FinancialRatio.drop('期別',1)
            FinancialRatio = FinancialRatio.apply(pd.to_numeric)
            FinancialRatio.columns = FinancialRatio.columns.str.strip()
            FinancialRatio.columns = FinancialRatio.columns.to_series().replace(["\(","\)"],["",""],regex=True)
            FinancialRatio.index = pd.Series(FinancialRatio.index).replace(["\.",'1Q','2Q','3Q','4Q'],["","01","04","07","10"],regex=True)
            FinancialRatio.index = pd.Series(FinancialRatio.index).apply(pd.to_numeric) - 191100
            FinancialRatio.reset_index(level=0, inplace=True)
            FinancialRatio['index'] = FinancialRatio['index'].astype(str)
            
            print('\n'+str(ID)+'\n'+'FinancialRatio Be Done')
            
            return(FinancialRatio)    
       
        
        
    '''
    Monthly Revenue
    Q1 YoY Revenue Growth near 3 Month is not zero?
    '''
    def MonthlyRevenueTable(self):
        ID = self.ID        
        
        Source = requests.get('http://jsjustweb.jihsun.com.tw/z/zc/zch/zch_%s.djhtm'%(ID)) 
        Source.encoding = 'Big5'
        
        Tree = etree.HTML(Source.text)
        Tree2 = Tree.xpath("/html//td[@class='t0']//td")
        
        Data = []
        for item in Tree2:
            Data.append(item.text)
        Data = Data[5:]
        
        if any(x == None for x in Data):
            print('\n'+str(ID)+'\n'+'Monthly Revenue Have None Data')
            Data = [np.NaN if x is None else x for x in Data]
        
        if any(x == 'N/A' for x in Data):
            print('\n'+str(ID)+'\n'+'Monthly Revenue Have NaN Data')
            Data = [np.NaN if x == 'N/A' else x for x in Data]
            
        length = len(Data)
        ColName = ['Date','ConsolidatedRevenues','MoMGrowth','ConsolidatedRevenuesLastYear',
                   'YoYGrowth','CumulativeRevenue','YoYCumulativeGrowth']
        
        MonthlyRevenue = pd.DataFrame()           
        for i in range(7,14,1):
            CName = ColName[i-7]
            TempData = pd.Series() 
            for j in range(i,length,7):
                TempData = TempData.append(pd.Series(Data[j]))
            MonthlyRevenue[CName] = TempData
        
        MonthlyRevenue = MonthlyRevenue.set_index('Date')
        
        #if any(MonthlyRevenue.isna().any()):
        #    print('\n'+str(ID)+'\n'+'some Monthly Revenue are N/A')
        if all(MonthlyRevenue.isna().any()):
            print('\n'+str(ID)+'\n'+'all Monthly Revenue are N/A')
            
        try:
            MonthlyRevenue = MonthlyRevenue.replace([',','%'],['',''],regex=True)
            MonthlyRevenue = MonthlyRevenue.apply(pd.to_numeric)
            MonthlyRevenue[['MoMGrowth','YoYGrowth','YoYCumulativeGrowth']] = MonthlyRevenue[['MoMGrowth','YoYGrowth','YoYCumulativeGrowth']]/100
            MonthlyRevenue.index = pd.Series(MonthlyRevenue.index).replace(["/"],[""],regex=True)
            print('\n'+str(ID)+'\n'+'MonthlyRevenue Be Done')
        except TypeError:
            print('\n'+str(ID)+'\n'+'All Monthly Revenue are N/A')
            MonthlyRevenue = pd.DataFrame()
        
        return(MonthlyRevenue)    
    
    
    '''
    Statement Of Cash Flow 
    Q1 3 Year Cumulative Cash Flow is Positive?
    '''
    def CashFlowTable(self):
        ID = self.ID        
        
        Source = requests.get('http://jsjustweb.jihsun.com.tw/z/zc/zc3/zc3_%s.djhtm'%(ID)) 
        Source.encoding = 'Big5'
        
        Tree = etree.HTML(Source.text)
        Tree2 = Tree.xpath("/html//td[@class='t0']//td")
        
        Data = []
        for item in Tree2:
            Data.append(item.text)
        Data = Data[1:]
        
        if any(x == None for x in Data):
            print('\n'+str(ID)+'\n'+'CashFlow Have None Data')
            Data = [np.NaN if x is None else x for x in Data]
        
        if any(x == 'N/A' for x in Data):
            print('\n'+str(ID)+'\n'+'CashFlow Have NaN Data')
            Data = [np.NaN if x == 'N/A' else x for x in Data]
        
        
        length = len(Data)
        
        if length < 290:
            print('\n'+str(ID)+'\n'+'CashFlow is not existed')
            CashFlow = pd.DataFrame()    
            return(CashFlow)
        else:
            
            if length == 361:
                NumOfPeriod = int((length-1)/40)
            elif length == 321:
                NumOfPeriod = int((length-1)/40)
            elif length == 291:
                NumOfPeriod = int((length-1)/95)
            elif length == 370:
                NumOfPeriod = int((length-1)/41)
            elif length == 379:
                NumOfPeriod = int((length-1)/42)
            elif (length == 388) & (ID != '3711' and ID != '3709'):
                NumOfPeriod = int((length-1)/43)
            elif (length == 388) & (ID == '3711' or ID == '3709'):  #股票3711的季資料期數不足
                NumOfPeriod = int((length-1)/95)
            elif length == 397:
                NumOfPeriod = int((length-1)/44)
            elif length == 406:
                NumOfPeriod = int((length-1)/45)
            elif length == 415:
                NumOfPeriod = int((length-1)/46)
            elif length == 423:
                NumOfPeriod = int((length-1)/47)
            elif length == 582:
                NumOfPeriod = int((length-1)/95)
            elif length == 679:
                NumOfPeriod = int((length-1)/95)
            elif length == 873:
                NumOfPeriod = int((length-1)/95)
            elif length == 1044:
                NumOfPeriod = int((length-1)/115)
            elif length == 1152:
                NumOfPeriod = int((length-1)/126)
            elif length == 1071:
                NumOfPeriod = int((length-1)/117)
            elif length == 1143:
                NumOfPeriod = int((length-1)/125)
            elif length == 776:
                NumOfPeriod = int((length-1)/95)
            
            if Data[96:97] == ["\u3000提列呆帳"]:
                NumOfPeriod = NumOfPeriod - 1
            
            ColName = Data[0:9]
                       
            CashFlow = pd.DataFrame()           
            for i in range(NumOfPeriod,NumOfPeriod*2,1):
                CName = ColName[i-NumOfPeriod]
                TempData = pd.Series() 
                for j in range(i,length,NumOfPeriod):
                    TempData = TempData.append(pd.Series(Data[j]))
                CashFlow[CName] = TempData
            
            
            CashFlow = CashFlow.set_index('期別')
            
            CashFlow = CashFlow.replace([',','%'],['',''],regex=True)
            CashFlow = CashFlow.transpose()
            del CashFlow['種類']
            CashFlow = CashFlow.apply(pd.to_numeric)
            CashFlow.columns = CashFlow.columns.str.strip()
            CashFlow.columns = CashFlow.columns.to_series().replace(["\(","\)"],["",""],regex=True)
            CashFlow.index = pd.Series(CashFlow.index).replace(["\.",'1Q','2Q','3Q','4Q'],["","01","04","07","10"],regex=True)
            CashFlow.index = pd.Series(CashFlow.index).apply(pd.to_numeric) - 191100
            CashFlow.reset_index(level=0, inplace=True)
            CashFlow['index'] = CashFlow['index'].astype(str)
            
            print('\n'+str(ID)+'\n'+'CashFlow Be Done')
            
            return(CashFlow)    
    
    
            
            
    '''        
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
    '''    
    
    def InformationAndPEHsitory(self):
        ID = self.ID        
        
        Source = requests.get('http://jsjustweb.jihsun.com.tw/z/zc/zca/zca_%s.djhtm'%(ID)) 
        Source.encoding = 'Big5'
        
        Tree = etree.HTML(Source.text)
        Tree2 = Tree.xpath("/html//td[@class='t0']//td")
        Tree3 = Tree.xpath("/html//table[@class='t01']//tr[17]//span[@class='t3n1']")
        Tree4 = Tree.xpath("/html/body//tr[18]/td[@class='t3n1'][1]/span[@class='t3n1']")
        Tree5 = Tree.xpath("/html//tr[1]/td[@class='t10']/div[@class='t11']")
        
        Data = []
        for item in Tree2:
           Data.append(item.text)
        Data2 = []
        for item in Tree3:
            Data2.append(item.text)
        Data3 = []
        for item in Tree4:
            Data3.append(item.text)
        Data4 = []
        for item in Tree5:
            Data4.append(item.text)
    
        CompanyInformation = pd.DataFrame(index=[str(str(dt.datetime.now().year)+ Data4[0][6:11]).replace("/","")])    
            
        CompanyInformation['PE'] = Data[17:18]
        CompanyInformation['IndustryPE'] = Data[25:26]
        CompanyInformation['YieldRate'] = Data[33:34]
        CompanyInformation['MarketValue'] = Data[35:36]    
        if Data[76:77][0]=='\xa0':
            CompanyInformation['PB'] = np.nan
        else:
            CompanyInformation['PB'] = Data[76:77]
        CompanyInformation['Beta'] = Data[49:50]
        CompanyInformation['SD'] = Data[57:58]
        CompanyInformation['ListingDate'] = Data[101:102]
        CompanyInformation['ShareCapital'] = Data[85:86]
        CompanyInformation['ClosePrice'] = Data[8:9]
        if Data2[:] == []:
            CompanyInformation['ROA'] = np.nan
        else:
            CompanyInformation['ROA'] = Data2[:]
        if Data3[:] == []:
            CompanyInformation['ROE'] = np.nan
        else:
            CompanyInformation['ROE'] = Data3[:]
        
        CompanyInformation = CompanyInformation.replace([",","%",'N/A'],["","",np.nan],regex=True)
        CompanyInformation.ix[:,[0,1,2,3,4,5,6,8,9,10,11]] = CompanyInformation.ix[:,[0,1,2,3,4,5,6,8,9,10,11]].apply(pd.to_numeric,)
        CompanyInformation[["YieldRate","SD","ROA","ROE"]] = CompanyInformation[["YieldRate","SD","ROA","ROE"]]/100
        Date = str(CompanyInformation.iloc[0]['ListingDate'])
        Date = Date.split('/')
        Date[0] = int(Date[0])+1911
        Date = str(Date[0])+str(Date[1])+str(Date[2])
        CompanyInformation['ListingDate'] = dt.datetime.strptime(Date,"%Y%m%d")
        print('\n'+str(ID)+'\n'+'CompanyInformation Be Done')
        
        #History PE Ratio
        HistoryPERatio = pd.DataFrame()        
        for i in range(142,197,9):
            CName = Data[i]
            TempData = pd.Series()
            for j in range(i+1,i+9):
                TempData = TempData.append(pd.Series(Data[j]))
            HistoryPERatio[CName] = TempData
        HistoryPERatio = HistoryPERatio.rename(columns={'年度':'Year','最高總市值':'MaxMarketValue',
                                                           '最低總市值':'MinMarketValue','最高本益比':'MaxPE',
                                                           '最低本益比':'MinPE','股票股利':'StockDividend',
                                                           '現金股利':'CashDividend'})
        HistoryPERatio = HistoryPERatio.set_index('Year')    
        HistoryPERatio = HistoryPERatio.replace(['N/A',','],[np.nan,''],regex=True)
        HistoryPERatio = HistoryPERatio.apply(pd.to_numeric)
        print('\n'+str(ID)+'\n'+'HistoryPERatio Be Done')
                            
        return(CompanyInformation,HistoryPERatio)


    def __str__(self):
        return("Stock ID is "+str(self.ID))
            
        

#dt.datetime.strptime('20101019','%Y%m%d') - dt.datetime.strptime('20101017','%Y%m%d')
       
#'''
#Test Data
# 
#with open('DD.txt','w') as D:
#    for i in StockNumber(KYDelete=True)['StkNum']:
#        try:
#            print(i+'\n',file=D)
#            [a,b] = InformationAndPEHsitory(i)
#        except:
#            print(i+'\n出現錯誤',file=D)
#            time.sleep(1)                
#            [a,b] = InformationAndPEHsitory(i)
#        else:
#            print(i+'\n二次出現錯誤',file=D) 
#            time.sleep(2)
#            [a,b] = InformationAndPEHsitory(i)
#''' 

