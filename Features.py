import mysql.connector
from sqlalchemy import create_engine
import pandas as pd
import pandas_ta as pta
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
        del mainIndex['index_name']
        mainIndex['date'] = pd.to_datetime(mainIndex['date'])
        mainIndex.sort_values(by='date', inplace=True)
        mainIndex.rename(columns={'price': 'close'}, inplace=True)

        # to pandas ta
        mainIndex = pta.utils.DataFrame(mainIndex)
        
        # EMA 21 55 233
        mainIndex.ta.ema(length=21, append=True)
        mainIndex.ta.ema(length=55, append=True)
        mainIndex.ta.ema(length=233, append=True)
        
        # RSI MACD
        mainIndex.ta.rsi(append=True)
        mainIndex.ta.macd(append=True)
        
        # MAX til 5 days ago
        mainIndex['max_close_5d'] = mainIndex['close'].shift(5).expanding().max()
        mainIndex['close2max_ratio'] = mainIndex['close']/mainIndex['max_close_5d']

        # PP
        pp_year = mainIndex[['date', 'close']]
        pp_year['year'] = pp_year['date'].dt.year
        #pp_year = pp_year.groupby(pp_year.year).close.apply(lambda x: x.ta.pivot(high=None, low=None, close=x, inplace=False))
        pp_year = pp_year.groupby('year').agg({'close': ['max', 'min', 'last']})
        pp_year.columns = ['high', 'low', 'close']
        pp_year = pp_year.shift(1)

        # Calculate the Pivot Point (PP)
        pp_year['PP'] = (pp_year['high'] + pp_year['low'] + 2 * pp_year['close']) / 4

        # Calculate the Support (S1, S2, S3) levels
        pp_year['S1'] = (2 * pp_year['PP']) - pp_year['high']
        pp_year['S2'] = pp_year['PP'] - (pp_year['high'] - pp_year['low'])
        pp_year['S3'] = pp_year['low'] - 2 * (pp_year['high'] - pp_year['PP'])

        # Calculate the Resistance (R1, R2, R3) levels
        pp_year['R1'] = (2 * pp_year['PP']) - pp_year['low']
        pp_year['R2'] = pp_year['PP'] + (pp_year['high'] - pp_year['low'])
        pp_year['R3'] = pp_year['high'] + 2 * (pp_year['PP'] - pp_year['low'])

        print(pp_year.tail(12))
        print(mainIndex.tail(12))

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
        mainInfo.sort_values(by='date', inplace=True)
        mainInfo = mainInfo.groupby('date').agg(aggCondition)
        
        print(mainInfo)
        
        """for index, row in mainIndex.iterrows():
            print(row['date'])
            dfTickerDate = self.dfTicker.loc[self.dfTicker.date == row['date']]
            print(dfTickerDate.deal_worth.sum())"""




Feature = Feature()
Feature.Main_Index()