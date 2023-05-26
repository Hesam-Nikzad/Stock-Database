import jdatetime
import datetime
import mysql.connector
import pytse_client as tse
from pytse_client.download import download_financial_indexes
from pytse_client import download_client_types_records
import os
import pandas as pd
from sqlalchemy import create_engine

class Ticker:
    def __init__(self):
        self.cnx = mysql.connector.connect(user='root', password='harchi',  host='127.0.0.1',  database='market')
        self.path = 'D:/Bourse/pytse_client/'
        self.cursor = self.cnx.cursor()
        self.engine = create_engine('mysql+pymysql://root:harchi@127.0.0.1:3306/market')
        self.Create_Tables()

    def Create_Tables(self):
        path = 'C:/Users/Hessum/OneDrive/Bourse/Stock Market Python/Financial Market Projects/Stock-Database/'

        for filename in os.listdir(path):
            
            if filename[-3:] != 'sql': continue
            filePath = os.path.join(path, filename)

            try:
                print(filePath)
                with open(filePath, 'r') as f: 
                    query = f.read()
                self.cursor.execute(query)
                self.cnx.commit()
                print('%s created' %filename)
            
            except: 
                print('%s already existed' %filename)

    def Update_All_CSV(self):
        tse.download(symbols="all", write_to_csv=True, include_jdate=True, base_path=self.path + 'adj', adjust=True)
        tse.download(symbols="all", write_to_csv=True, include_jdate=True, base_path=self.path + 'notAdj', adjust=False)
        download_financial_indexes(symbols="all", write_to_csv=True, base_path=self.path + 'indices', include_jdate=True)
        download_client_types_records("all",  write_to_csv=True, base_path=self.path + 'client_types', include_jdate=True)

    def Update_Departed_CSV(self):
        now = datetime.datetime.now()
        timedelta = datetime.timedelta(days=1)
        i = 0
        
        for folder in os.listdir(self.path):
            folderDir = os.path.join(self.path, folder) + '/'
            
            for file in os.listdir(folderDir):
                filePath = os.path.join(folderDir, file)
                modified_time  = (int(os.path.getmtime(filePath)))
                modified_time = datetime.datetime.fromtimestamp(modified_time)
                
                try:
                    if modified_time < now - timedelta:
                        fileName = file[:file.find('.') if file.find('-') == -1 else file.find('-')]
                        print(modified_time , fileName, folder)
                        i += 1
                        if folder == 'adj':
                            tse.download(symbols=fileName, write_to_csv=True, include_jdate=True, base_path=folderDir, adjust=True)
                        
                        elif folder == 'NotAdj':
                            tse.download(symbols=fileName, write_to_csv=True, include_jdate=True, base_path=folderDir, adjust=False)
                        
                        elif folder == 'client_types':
                            download_client_types_records(fileName,  write_to_csv=True, base_path=folderDir, include_jdate=True)
                        
                        elif folder == 'indices':
                            download_financial_indexes(fileName, write_to_csv=True, base_path=folderDir, include_jdate=True)
                except:
                    pass
        return i

    def Tickers_list(self):
        self.tickerList = []
        path = self.path + 'notAdj/'
        for filename in os.listdir(path):
            if os.path.isfile(os.path.join(path, filename)):
                filename = os.path.splitext(filename)[0]
                try:
                    int(filename)            
                except:
                    self.tickerList.append(filename)

    def Indices_list(self):
        self.indicesList = []
        path = self.path + 'indices/'
        for filename in os.listdir(path):
            if os.path.isfile(os.path.join(path, filename)):
                filename = os.path.splitext(filename)[0]
                try:
                    int(filename)            
                except:
                    self.indicesList.append(filename)

    def Sweep_Tickers(self):
        self.Tickers_list()
        self.cursor.execute('TRUNCATE TABLE ticker_data;')
        self.cnx.commit()

        for ticker in self.tickerList:
            pathAdj = self.path + 'adj/%s.csv' %(ticker + '-ت')
            pathNotAdj = self.path + 'notAdj/%s.csv' %ticker
            pathClient = self.path + 'client_types/%s.csv' %ticker
            
            if not os.path.exists(pathClient): continue

            # Adjusted prices
            dfAdj = pd.read_csv(pathAdj)
            del dfAdj['jdate']
            dfAdj.rename(columns={'adjClose': 'adj_close', 
                                    'low':'adj_low', 
                                    'high': 'adj_high',
                                    'open': 'adj_open',
                                    'close': 'adj_mean',
                                    'yesterday': 'adj_yesterday_mean',
                                    'count': 'deal_count',
                                    'value': 'deal_worth'}, inplace=True)

            # Not adjusted prices
            dfNotAdj = pd.read_csv(pathNotAdj)
            del dfNotAdj['jdate']
            del dfNotAdj['volume']
            del dfNotAdj['value']
            del dfNotAdj['count']
            dfNotAdj.rename(columns={'adjClose': 'not_adj_close', 
                                    'low':'not_adj_low', 
                                    'high': 'not_adj_high',
                                    'open': 'not_adj_open',
                                    'close': 'not_adj_mean',
                                    'yesterday': 'not_adj_yesterday_mean'}, inplace=True)

            # Individual and corporate dealers
            dfClient = pd.read_csv(pathClient)
            del dfClient['Unnamed: 0']
            del dfClient['jdate']

            # Merge all DataFrames together
            merged_df = pd.merge(dfAdj, dfNotAdj, on='date')
            merged_df = pd.merge(dfClient, merged_df, on='date')
            merged_df['date'] = pd.to_datetime(merged_df['date'])
            merged_df['ticker'] = ticker

            merged_df.to_sql(name='ticker_data', con=self.engine, if_exists='append', index=False, chunksize=1000)
            print('%s data has been imported' %ticker)

    def Sweep_Indices(self):
        self.Indices_list()
        self.cursor.execute('TRUNCATE TABLE index_data;')
        self.cnx.commit()

        for index in self.indicesList:
            path = self.path + 'indices/%s.csv' %index
            dfIndex = pd.read_csv(path)
            del dfIndex['jdate']
            dfIndex['index_name'] = index
            dfIndex.rename(columns={'value': 'price'}, inplace=True)
            #print(dfIndex)
            dfIndex.to_sql(name='index_data', con=self.engine, if_exists='append', index=False, chunksize=1000)
            print('%s data has been imported' %index)

T = Ticker()
#T.Update_Departed_CSV()
T.Update_All_CSV()
#T.Tickers_list()
#T.Sweep_Tickers()
#T.Sweep_Indices()