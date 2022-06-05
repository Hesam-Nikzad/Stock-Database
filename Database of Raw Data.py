import jdatetime
import mysql.connector

def Symbol_Candle_Read_Adj (EnglishSymbol):

    path = "D:\\Bourse\\Advanced Get 9.1\\Noavaran Amin\\NoavaranAmin\\ExportedFile\\Adj\\" + EnglishSymbol + ".txt"
    f = open(path, 'r')
    lines = f.readlines()

    data = list()

    for i in range(1, len(lines)):
        line = lines[i]
        line = line.strip()
        a = line.find(',')

        a = a + 3
        line = line[a:]
        line = line[:8] + line[15:]                                             # Making arrays strip
        values = [int(j) for j in line.split(',')]
        data.append(values)                                 # data = [date, open, high, low, close, volume]

    return data

def Symbol_Candle_Read_NotAdj (EnglishSymbol):

    path = "D:\\Bourse\\Advanced Get 9.1\\Noavaran Amin\\NoavaranAmin\\ExportedFile\\NotAdj\\" + EnglishSymbol + "_NAdj.txt"
    f = open(path, 'r')
    lines = f.readlines()

    data = list()

    for i in range(1, len(lines)):
        line = lines[i]
        line = line.strip()
        a = line.find(',')

        a = a + 3
        line = line[a:]
        line = line[:8] + line[15:]                                             # Making arrays strip
        values = [int(j) for j in line.split(',')]
        data.append(values)                                 # data = [date, open, high, low, close, volume]

    return data

def RLinfo (DateShamsi):
    path3 = 'C:\\Users\\Hessum\\OneDrive\\Bourse\\Raw Data\\%s.txt' %DateShamsi
    ff=open(path3, 'r')
    lines = ff.readlines()
    RL = list()

    for i in range(1, len(lines)):
        line = lines[i]
        line = line.strip()
            
        values = line.split(',')
        RL.append(values)
    
    return RL


# The first date which we have candle data about it is 1380/01/05
# The first date which we have Real-Legal data about it is 1393/01/05
JalaliDate_Start = jdatetime.date(1393, 1, 5)                                   # Jalali Start Date
JalaliDate_Stop = jdatetime.date(1401, 3, 6)                                    # Jalali Stop Date

GregorianDate_start = jdatetime.date.togregorian(JalaliDate_Start)              # Convert Start Date to Gregorian 
GregorianDate_stop = jdatetime.date.togregorian(JalaliDate_Stop)                # Convert Start Date to Gregorian Stop Date

OneDayDelta = jdatetime.timedelta(days=1)                                       # Make a one-day delta time

cnx = mysql.connector.connect(user='root', password='harchi',
                              host='127.0.0.1',
                              database='market')

cursor = cnx.cursor()

cursor.execute('SELECT EnglishSymbol FROM companies;')
CompaniesName = cursor.fetchall()

for CompanyName in CompaniesName:

    EnglishSymbol = CompanyName[0]
    Adj_Price_Data = Symbol_Candle_Read_Adj (EnglishSymbol)                 # Adjusted price data = [date, open, high, low, close, volume]
    NotAdj_Price_Data = Symbol_Candle_Read_NotAdj (EnglishSymbol)           # Not Adjusted data = [date, open, high, low, close, volume]


cnx.close()
