import os
import sys
current_dir = os.getcwd()
sys.path.append(current_dir)
from TSETMC import *
from time import sleep

T = Ticker()

try:
    T.Update_All_CSV()
except:
    pass

notUpdated = 100
i=0
while notUpdated > 0:
    
    i+=1
    notUpdated = T.Update_Departed_CSV()
    print(notUpdated)
    sleep(300)
    if i==10:
        break
    

T.Sweep_Tickers()
T.Sweep_Indices()