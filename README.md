# FundamentalAnalysis
  分析的股票為所有台灣上市櫃公司，其中包含爬取所有上市櫃公司股票、更新資料庫(MySQL)、參數調整、合理股價估值、選股策略。

主要分為以下幾個部分:
•	CrawlDataFromWeb --- 其中包含FinancialStatement.py與UpdateDataToMySQL.py這兩個檔案
    (1) 「FinancialStatement.py」可以將目前台灣股市上市上櫃的代碼從 http://isin.twse.com.tw/isin/C_public.jsp?strMode=2 爬取下來，並利用這個代碼        到 http://jsjustweb.jihsun.com.tw/z/zc/zce/zce_%s.djhtm 爬取所有相應的公司財務報表，包括損益表、獲利能力指標、月營收、現金流量等。
    (2) 「UpdateDataToMySQL.py」將抓下來的最新財報資料與我MySQL資料庫中的歷史資料合併與統一格式，其中包括每日股價的抓取，上市公司從                          https://finance.yahoo.com/quote/{0}.TW/history 抓取，上櫃公司從 https://finance.yahoo.com/quote/{0}.TWO/history 抓取。
 
•	FundamentalStrategy --- 其中包含FindParameter.py、LongTermValueStrategy.py、ReasonablePrice.py與SelectStock.py四個檔案
    (1) 「FindParameter.py」是從近6個月漲幅最高的100名公司財報資訊中，得到一個選股策略的最佳參數
    (2) 「LongTermValueStrategy.py」是整個基本分析策略的核心，可以通過設定月營收年成長率為正的月數、營業利益率波動4%以下、存貨周轉率等參數，選出符合         財報條件的股票。
    (3) 「ReasonablePrice.py」利用公式LastYearEPS * (1 + RevenueAvgYoY) * (1 + OperatingAvgYoY) * MinimumPE * Beta 預估股票的合理股價。
    (4) 「SelectStock.py」整合上述所有程式代碼，選出投資股票代碼。
     
•	TechniqueStrategy --- 包含MomentumStrategy.py
    (1) 「MomentumStrategy.py」一個簡單的股票動能策略
