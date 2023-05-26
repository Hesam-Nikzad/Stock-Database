from ctypes import sizeof
import mysql.connector
import pandas as pd
import talib

class Stock:
    def __init__(self, EnglishSymbol):
        
        query = 'SELECT * FROM stock_raw_data WHERE EnglishSymbol = \'%s\';' %EnglishSymbol
        self.Daily_Raw_Data = pd.read_sql(query, cnx, parse_dates=["Gregorian_Date"])
        
    def to_weekly(self):
        
        logic = {
            'EnglishSymbol':'first',
            'Open_NotAdj': 'first',
            'High_NotAdj': 'max',
            'Low_NotAdj': 'min',
            'Close_NotAdj': 'last',
            'Open_Adj': 'first',
            'High_Adj': 'max',
            'Low_Adj' : 'min',
            'Close_Adj': 'last',
            'Total_Volume': 'sum',
            'Real_Sell_Number': 'sum',
            'Real_Sell_Volume': 'sum',
            'Legal_Sell_Volume': 'sum',
            'Legal_Sell_Number': 'sum',
            'Real_Buy_Volume': 'sum',
            'Real_Buy_Number': 'sum',
            'Legal_Buy_Number': 'sum',
            'Legal_Buy_Volume':'sum',
            'Closing_NotAdj': 'last'
            }

        self.Weekly_df = self.Daily_Raw_Data.resample('W', on='Gregorian_Date').apply(logic)
        self.Weekly_df.index = self.Weekly_df.index - pd.tseries.frequencies.to_offset("6D")

        return self.Weekly_df.dropna()

    def to_monthly(self):

        logic = {
            'EnglishSymbol':'first',
            'Open_NotAdj': 'first',
            'High_NotAdj': 'max',
            'Low_NotAdj': 'min',
            'Close_NotAdj': 'last',
            'Open_Adj': 'first',
            'High_Adj': 'max',
            'Low_Adj' : 'min',
            'Close_Adj': 'last',
            'Total_Volume': 'sum',
            'Real_Sell_Number': 'sum',
            'Real_Sell_Volume': 'sum',
            'Legal_Sell_Volume': 'sum',
            'Legal_Sell_Number': 'sum',
            'Real_Buy_Volume': 'sum',
            'Real_Buy_Number': 'sum',
            'Legal_Buy_Number': 'sum',
            'Legal_Buy_Volume':'sum',
            'Closing_NotAdj': 'last'
            }

        self.Monthly_df = self.Daily_Raw_Data.resample('M', on='Gregorian_Date').apply(logic)
        self.Monthly_df.index = self.Monthly_df.index - pd.tseries.frequencies.to_offset("1M")

        return self.Monthly_df
    
    def RSI(self, df_in):
        return talib.RSI(df_in.Close_Adj, timeperiod=14)



cnx = mysql.connector.connect(user='root', password='harchi', host='127.0.0.1', database='market')
cursor = cnx.cursor()

query = 'SELECT * FROM companies;'
Companies_df = pd.read_sql(query, cnx).reset_index()

for index, row in Companies_df.iterrows():
    
    print(row['EnglishSymbol'])
    Stock = Stock(row['EnglishSymbol'])                                             # Import the Stock's data from SQL
    Daily_Raw_Data = Stock.Daily_Raw_Data                                           # Daily Raw data of the Stock
    Weekly_Raw_Data = Stock.to_weekly()                                             # Weekly Raw data of the Stock
    Monthly_Raw_Data = Stock.to_monthly()                                           # Weekly Raw data of the Stock

    Stock_Daily = pd.DataFrame()
    Stock_Daily['Gregorian_Date'] = Daily_Raw_Data.Gregorian_Date                   # Empty dataframe for daily proccessed data
    Stock_Weekly = pd.DataFrame()
    Stock_Weekly['Gregorian_Date'] = Weekly_Raw_Data.index                          # Empty dataframe for weekly proccessed data
    Stock_Monthly = pd.DataFrame()
    Stock_Monthly['Gregorian_Date'] = Monthly_Raw_Data.index                        # Empty dataframe for Monthly proccessed data

    Stock_Daily['RSI_Daily'] = Stock.RSI(Daily_Raw_Data)                                                    # Calculate  daily RSI
    Stock_Weekly['RSI_Weekly'] = talib.RSI(Weekly_Raw_Data.reset_index()['Close_Adj'], timeperiod=14)       # Calculate  Weekly RSI
    Stock_Monthly['RSI_Monthly'] = talib.RSI(Monthly_Raw_Data.reset_index()['Close_Adj'], timeperiod=14)    # Calculate  Monthly RSI

    print(Stock_Monthly)
    
    break