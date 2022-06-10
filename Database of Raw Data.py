import jdatetime
import datetime
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

def Date_obj2int(date):
    return 10000*date.year + 100*date.month + date.day

def Find_Price_And_Date(date_int, StockPrice):
    output = None

    for line in StockPrice:
        if line[0] == date_int:
            output = line[1:]               # output = [open, high, low, close, volume]

    return output

def Real_Legal_Info (EnglishSymbol, date_int):
    
    path = 'C:\\Users\\Hessum\\OneDrive\\Bourse\\Raw Data\\%s.txt' %date_int
    ff=open(path, 'r')
    lines = ff.readlines()
    RL = [None, None, None, None, None, None, None, None, None]

    for i in range(1, len(lines)):
        line = lines[i]
        line = line.strip()     
        values = line.split(', ')

        if values[0] == EnglishSymbol:
            RL = [int(j) for j in values[1:]]
    
    return RL

def Start_Date_Finder(Stock):
    
    Released_Date_int = Adj_Price_Data[0][0]
    y = Released_Date_int//10**4
    m = (Released_Date_int%10**4)//10**2
    d = Released_Date_int % 10**2
    Released_Date = datetime.date(y, m, d)                      # Released date of the stock

    Start_Date = max(GregorianDate_start, Released_Date)

    cursor.execute('SELECT max(Gregorian_Date) FROM raw_data WHERE EnglishSymbol = \'%s\';' %Stock)
    result = cursor.fetchall()
    
    if result[0][0] != None:
        Start_Date = result[0][0] + OneDayDelta                 # If this stock data were inserted 
    
    return Start_Date

# The first date which we have candle data about it is 1380/01/05
# The first date which we have Real-Legal data about it is 1393/01/05
JalaliDate_Start = jdatetime.date(1393, 1, 5)                                   # Jalali Start Date
JalaliDate_Stop = jdatetime.date(1401, 3, 20)                                    # Jalali Stop Date

GregorianDate_start = jdatetime.date.togregorian(JalaliDate_Start)              # Convert Start Date to Gregorian 
GregorianDate_stop = jdatetime.date.togregorian(JalaliDate_Stop)                # Convert Start Date to Gregorian Stop Date

OneDayDelta = datetime.timedelta(days=1)                                       # Make a one-day delta time

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
    GregorianDate = Start_Date_Finder(EnglishSymbol)
    print(EnglishSymbol, GregorianDate)

    while GregorianDate <= GregorianDate_stop:
        
        Stock_Date_Info = []

        JalaliDate = jdatetime.datetime.fromgregorian(datetime=GregorianDate)            # convert Jalali date to Gregorian
        Stock_Date_Info.append(EnglishSymbol)                               # add English symbol of stock to the list
        Stock_Date_Info.append(GregorianDate.isoformat())                   # add Gregorian Date to the list
        
        JalaliDate_int = Date_obj2int(JalaliDate)                           # convert Jalali date as an object to an integer
        GregorianDate_int = Date_obj2int(GregorianDate)                     # convert Gregorian date as an object to an integer
        
        Adj_Price = Find_Price_And_Date(GregorianDate_int, Adj_Price_Data)          # adjusted price data of a specific date
        NotAdj_Price = Find_Price_And_Date(GregorianDate_int, NotAdj_Price_Data)    # not adjusted price data of a specific date

        if Adj_Price == None or NotAdj_Price == None:                       # if there isn't any data of that date skip it 
            GregorianDate += OneDayDelta
            continue
        
        Stock_Date_Info.extend(NotAdj_Price[:-1])                           # add not adjusted open high low close prices to the list
        Stock_Date_Info.extend(Adj_Price)                                   # add adjusted open high low close prices and total volume to the list
        
        Real_Legal_Volume_Number = Real_Legal_Info (EnglishSymbol, JalaliDate_int)  # Real Legal Number Volume and Closing Price
        Stock_Date_Info.extend(Real_Legal_Volume_Number)                            # Closing price in Tehran Stocks is something average of dealed price of each day
        
        format_strings = ','.join(['%s'] * len(Stock_Date_Info))
        cursor.execute('INSERT INTO raw_data VALUES (%s)' % format_strings, tuple(Stock_Date_Info))
        cnx.commit()

        GregorianDate += OneDayDelta

cnx.close()
