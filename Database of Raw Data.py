import jdatetime


# The first date which we have candle data about it is 1380/01/05
# The first date which we have Real-Legal data about it is 1393/01/05
JalaliDate_Start = jdatetime.date(1393, 1, 5)                                   # Jalali Start Date
JalaliDate_Stop = jdatetime.date(1401, 3, 6)                                    # Jalali Stop Date

GregorianDate_start = jdatetime.date.togregorian(JalaliDate_Start)              # Convert Start Date to Gregorian 
GregorianDate_stop = jdatetime.date.togregorian(JalaliDate_Stop)                # Convert Start Date to Gregorian Stop Date

OneDayDelta = jdatetime.timedelta(days=1)                                       # Make a one-day delta time


print(OneDayDelta)
