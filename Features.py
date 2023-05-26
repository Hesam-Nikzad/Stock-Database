import mysql.connector
from sqlalchemy import create_engine
import pandas as pd
import ta
import numpy as np


class Feature:
    def __init__(self):
        self.cnx = mysql.connector.connect(user='root', password='harchi',  host='127.0.0.1',  database='market')
        self.cursor = self.cnx.cursor()
        self.engine = create_engine('mysql+pymysql://root:harchi@127.0.0.1:3306/market')
        self.dfTicker = pd.read_sql('SELECT * FROM ticker_data', self.cnx)
        self.dfIndex = pd.read_sql('SELECT * FROM index_data', self.cnx)

    def Main_Index(self):
        mainIndex = self.dfIndex.loc[self.dfIndex.index_name=='شاخص كل']
        # Compute RSI
        rsi_period = 14  # Number of periods for RSI calculation
        mainIndex['rsi'] = ta.momentum.RSIIndicator(close=mainIndex['price'], window=rsi_period).rsi()
        mainIndex['rsi'] = mainIndex['rsi'].apply(lambda x: int(x) if not np.isnan(x) else 0)
        print(mainIndex)

        columns = ['date', 'deal_worth', 'deal_count', 'volume', 'individual_buy_value',
                   'individual_buy_count', 'corporate_buy_value', 'corporate_buy_count']
        aggCondition = {
            'deal_worth': 'sum', 
            'deal_count': 'sum', 
            'volume': 'sum',
            'individual_buy_value': 'sum',
            'individual_buy_count': 'sum',
            'corporate_buy_value': 'sum',
            'corporate_buy_count': 'sum'
        }
        mainInfo = self.dfTicker[columns]
        mainInfo = mainInfo.groupby('date').agg(aggCondition)
        mainInfo.sort_values(by='date', inplace=True)
        print(mainInfo)
        
        """for index, row in mainIndex.iterrows():
            print(row['date'])
            dfTickerDate = self.dfTicker.loc[self.dfTicker.date == row['date']]
            print(dfTickerDate.deal_worth.sum())"""




Feature = Feature()
Feature.Main_Index()