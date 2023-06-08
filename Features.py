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
        self.path = 'C:/Users/Hessum/OneDrive/Bourse/Stock Market Python/Financial Market Projects/Stock-Database/'

    def Main_Index(self):
        mainIndex = self.dfIndex.loc[self.dfIndex.index_name=='شاخص كل']
        del mainIndex['index_name']
        mainIndex['date'] = pd.to_datetime(mainIndex['date'])
        mainIndex.sort_values(by='date', inplace=True)
        mainIndex.rename(columns={'price': 'close'}, inplace=True)

        # to pandas_ta
        mainIndex = pta.utils.DataFrame(mainIndex)
        
        # EMA 21 55 233
        mainIndex.ta.ema(length=21, append=True)
        mainIndex.ta.ema(length=55, append=True)
        mainIndex.ta.ema(length=233, append=True)

        mainIndex['EMA21_Close'] = mainIndex['EMA_21']/mainIndex['close']
        mainIndex['EMA55_Close'] = mainIndex['EMA_55']/mainIndex['close']
        mainIndex['EMA233_Close'] = mainIndex['EMA_233']/mainIndex['close']

        mainIndex['EMA21_EMA55'] = mainIndex['EMA_21']/mainIndex['EMA_55']
        mainIndex['EMA21_EMA233'] = mainIndex['EMA_21']/mainIndex['EMA_233']
        mainIndex['EMA21_EMA233'] = mainIndex['EMA_55']/mainIndex['EMA_233']

        mainIndex.drop(columns=['EMA_21', 'EMA_55', 'EMA_233'], inplace=True)

        
        # RSI MACD
        mainIndex.ta.rsi(append=True)
        mainIndex.ta.macd(append=True)

        mainIndex['MACD_Close'] = mainIndex['MACD_12_26_9']/mainIndex['close']
        mainIndex['MACDh_Close'] = mainIndex['MACDh_12_26_9']/mainIndex['close']
        mainIndex['MACDs_Close'] = mainIndex['MACDs_12_26_9']/mainIndex['close']

        mainIndex.drop(columns=['MACD_12_26_9', 'MACDh_12_26_9', 'MACDs_12_26_9'], inplace=True)
        
        # MAX close til 5 days ago
        mainIndex['max_close_5d'] = mainIndex['close'].shift(5).expanding().max()
        mainIndex['close_max'] = mainIndex['close']/mainIndex['max_close_5d']

        mainIndex.drop(columns=['max_close_5d'], inplace=True)

        # PP strategy
        pp_year = mainIndex[['date', 'close']]
        pp_year['year'] = pp_year['date'].dt.year
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

        pp_year.clip(lower=0, inplace=True)
        pp_year.drop(columns=['high', 'low', 'close'], inplace=True)

        # Merge mainIndex and PP
        mainIndex = mainIndex.merge(pp_year, left_on=mainIndex['date'].dt.year, right_on=pp_year.index, how='left')
        mainIndex.drop(columns=['key_0'], inplace=True)
        
        mainIndex['PP_Close'] = mainIndex['PP']/mainIndex['close']
        mainIndex['R1_Close'] = mainIndex['R1']/mainIndex['close']
        mainIndex['R2_Close'] = mainIndex['R2']/mainIndex['close']
        mainIndex['R3_Close'] = mainIndex['R3']/mainIndex['close']
        mainIndex['S1_Close'] = mainIndex['S1']/mainIndex['close']
        mainIndex['S2_Close'] = mainIndex['S2']/mainIndex['close']
        mainIndex['S3_Close'] = mainIndex['S3']/mainIndex['close']

        mainIndex.drop(columns=['PP', 'R1', 'R2', 'R3', 'S1', 'S2', 'S3'], inplace=True)

        # Smart Money
        cols = ['date', 'deal_worth', 'deal_count', 'volume', 'individual_buy_value', 'individual_buy_count', 
                   'individual_sell_value', 'individual_sell_count', 'corporate_buy_value', 'corporate_buy_count',
                   'corporate_sell_value', 'corporate_sell_count']
        aggCondition = {
            'deal_worth': 'sum', 
            'deal_count': 'sum', 
            'volume': 'sum',
            'individual_buy_value': 'sum',
            'individual_buy_count': 'sum',
            'individual_sell_value': 'sum',
            'individual_sell_count': 'sum',
            'corporate_buy_value': 'sum',
            'corporate_buy_count': 'sum',
            'corporate_sell_value': 'sum',
            'corporate_sell_count': 'sum'
        }
        mainInfo = self.dfTicker[cols]
        mainInfo.sort_values(by='date', inplace=True)
        mainInfo = mainInfo.groupby('date').agg(aggCondition)

        # individual and corporate buyer power
        mainInfo['individial_buyer_power'] = (mainInfo['individual_buy_value']/mainInfo['individual_buy_count'])/(mainInfo['individual_sell_value']/mainInfo['individual_sell_count'])
        mainInfo['corporate_buyer_power'] = (mainInfo['corporate_buy_value']/mainInfo['corporate_buy_count'])/(mainInfo['corporate_sell_value']/mainInfo['corporate_sell_count'])

        # individual and corporate money income
        mainInfo['individual_money_in'] = mainInfo['individual_buy_value'] - mainInfo['individual_sell_value']
        mainInfo['individual_money_21'] = mainInfo['individual_money_in']/mainInfo['individual_money_in'].rolling(window=21).mean()
        mainInfo['individual_money_55'] = mainInfo['individual_money_in']/mainInfo['individual_money_in'].rolling(window=55).mean()
        mainInfo['individual_money_233'] = mainInfo['individual_money_in']/mainInfo['individual_money_in'].rolling(window=233).mean()

        # average volume
        mainInfo['mean_volume_21'] = mainInfo['volume']/mainInfo['volume'].rolling(window=21).mean()
        mainInfo['mean_volume_55'] = mainInfo['volume']/mainInfo['volume'].rolling(window=55).mean()
        mainInfo['mean_volume_233'] = mainInfo['volume']/mainInfo['volume'].rolling(window=233).mean()
        
        # average deal worth
        mainInfo['mean_worth_21'] = mainInfo['deal_worth']/mainInfo['deal_worth'].rolling(window=21).mean()
        mainInfo['mean_worth_55'] = mainInfo['deal_worth']/mainInfo['deal_worth'].rolling(window=55).mean()
        mainInfo['mean_worth_233'] = mainInfo['deal_worth']/mainInfo['deal_worth'].rolling(window=233).mean()
        

        print(mainInfo)
        # Clean mainInfo and merge it to mainIndex
        cols.remove('date')
        mainInfo.drop(columns=cols, inplace=True)
        mainIndex = mainIndex.set_index('date').join(mainInfo)
        # Save on local
        mainIndex.to_csv(self.path + 'mainIndex.csv')



Feature = Feature()
Feature.Main_Index()