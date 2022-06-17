import jdatetime
import datetime
import mysql.connector
from numpy import append

def Symbol_Candle_Read (EnglishSymbol):

    path = "D:\\Bourse\\Advanced Get 9.1\\Noavaran Amin\\NoavaranAmin\\ExportedFile\\Index\\" + EnglishSymbol + ".txt"
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
        values = [int(float(j)) for j in line.split(',')]
        data.append(values)                                 # data = [date, open, high, low, close, volume]

    return data

def Start_Date_Finder(Stock):
    
    Released_Date_int = Price_Data[0][0]
    y = Released_Date_int//10**4
    m = (Released_Date_int%10**4)//10**2
    d = Released_Date_int % 10**2
    Released_Date = datetime.date(y, m, d)                      # Released date of the index

    Start_Date = max(GregorianDate_start, Released_Date)

    cursor.execute('SELECT max(Gregorian_Date) FROM raw_data WHERE EnglishSymbol = \'%s\';' %Stock)
    result = cursor.fetchall()
    
    if result[0][0] != None:
        Start_Date = result[0][0] + OneDayDelta                 # If this stock data were inserted 
    
    return Start_Date

def Date_obj2int(date):
    return 10000*date.year + 100*date.month + date.day

def Find_Price_And_Date(date_int, StockPrice):
    output = None

    for line in StockPrice:
        if line[0] == date_int:
            output = line[1:]               # output = [open, high, low, close, volume]

    return output


# The first date which we have candle data about it is 1380/01/05
# The first date which we have Real-Legal data about it is 1393/01/05
JalaliDate_Start = jdatetime.date(1387, 1, 1)                                   # Jalali Start Date
JalaliDate_Stop = jdatetime.date(1401, 3, 27)                                   # Jalali Stop Date

GregorianDate_start = jdatetime.date.togregorian(JalaliDate_Start)              # Convert Start Date to Gregorian 
GregorianDate_stop = jdatetime.date.togregorian(JalaliDate_Stop)                # Convert Start Date to Gregorian Stop Date

OneDayDelta = datetime.timedelta(days=1)                                       # Make a one-day delta time

cnx = mysql.connector.connect(user='root', password='harchi',
                              host='127.0.0.1',
                              database='market')

cursor = cnx.cursor()

cursor.execute('SELECT * FROM industries WHERE AddressName IS NOT NULL;')
Industries = cursor.fetchall()

for Industry in Industries:

    AddressName = Industry[3]
    IndustryID = int(Industry[0])
    Price_Data = Symbol_Candle_Read (AddressName)
    GregorianDate = Start_Date_Finder(AddressName)
    print(AddressName, GregorianDate)
    
    while GregorianDate <= GregorianDate_stop:

        Industry_Date_Info = []

        JalaliDate = jdatetime.datetime.fromgregorian(datetime=GregorianDate)            # convert Jalali date to Gregorian
        Industry_Date_Info.append(IndustryID)                                   # add Industry ID
        Industry_Date_Info.append(AddressName)                                  # add English symbol of industry to the list
        Industry_Date_Info.append(GregorianDate.isoformat())                    # add Gregorian Date to the list
            
        JalaliDate_int = Date_obj2int(JalaliDate)                           # convert Jalali date as an object to an integer
        GregorianDate_int = Date_obj2int(GregorianDate)                     # convert Gregorian date as an object to an integer
        Price = Find_Price_And_Date(GregorianDate_int, Price_Data)          # adjusted price data of a specific date

        if Price == None:                                                   # if there isn't any data of that date skip it 
            GregorianDate += OneDayDelta
            continue

        Industry_Date_Info.extend(Price)                                       # add adjusted open high low close prices and total volume to the list
        format_strings = ','.join(['%s'] * len(Industry_Date_Info))
        cursor.execute('INSERT INTO index_raw_data VALUES (%s)' % format_strings, tuple(Industry_Date_Info))
        cnx.commit()

        GregorianDate += OneDayDelta