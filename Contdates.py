import os
import numpy as np
import pandas as pd
import datetime as dt
import itertools

#Change the base path before executing code
Base_path = "/home/tennistetris/Documents/Data Analytics/Hall/Inter_hall"
data1 = "NAV_data"
data2 = "Mix"
Path_to_data = os.path.join(Base_path,data1,data2)
files = os.listdir(Path_to_data)
os.chdir(Path_to_data)
Write_path = os.path.join(Base_path,'NAV_quarterly') 

def Continuous_dates(Values):
    '''
    This function takes an intermittent pandas series indexed by date 
    and converts it to a continuous pandas series where missing values
    are replaced by the Naive Method (prev. value)    
    '''
    Index  = Values.index
    Start_date = Index[0]
    End_date = Index[len(Index)-1]
    Num_dates = (End_date - Start_date).days
    Cont_dates = pd.date_range(Start_date ,End_date)
    ts = pd.Series(np.zeros(len(Cont_dates)),index = Cont_dates)
    i = 0;j = 0
    while(j<len(Index)):
        #print (i,' : ',Cont_dates[i]);print(j,' : ',Index[j])
        if((Cont_dates[i] - Index[j]).days == 0):
            ts[i] = Values.iloc[j] 
            i = i+1;j=j+1
        elif((Cont_dates[i] - Index[j]).days < 0):
            ts[i] = Values.iloc[j-1]
            i = i+1
    return ts

List1 = ['01' , '04' , '07' , '10']
List2 = ['2013' , '2014' , '2015' , '2016' , '2017']
Quarters = list(itertools.product(List1,List2))

List3 = list()

for i in range(len(Quarters)):
    temp = Quarters[i]
    temp = temp[1]+'-'+temp[0]
    temp = pd.Period(temp,'Q')
    List3.append(temp)
    
    
Quarters = pd.Series(List3).sort_values().reset_index(drop = True)

os.chdir(Path_to_data)
files = os.listdir(Path_to_data)

Required_files = []
os.chdir(Base_path)

db_temp1 = pd.read_csv('MF_IIT_DATA_V1_EDITED.csv')
db_temp2 = pd.read_csv('MF_IIT_DATA_V3_EDITED.csv')

IDs = db_temp2['ID']
#Removing NaNs
for i in range(len(IDs)):
    try:
        IDs[i] = int(IDs[i])
    except ValueError:
        IDs[i] = 0

        
for file in files:
    #Reading Data
    os.chdir(Path_to_data)
    db = pd.read_csv(file)
    #Indexing the dates
    Index = list()
    for i in range(db.shape[0]):
        Index.append(dt.datetime.strptime(db.iloc[i,6],'%d-%b-%Y'))
    NAV = pd.Series(db[db.columns[3]])
    NAV.index = Index
    NAV = Continuous_dates(NAV)  # To convert to continupus timeseries data
    #Finding the Quarterly Returns
    Quarterly_Returns = pd.Series(np.zeros(len(NAV)),index =NAV.index)
    Time_lag = 91 #Since we are calculating Quaterly Returns
              #Each quarter is assumed to have 91 days
    for i in range(len(NAV)):
        t2 = i
        t1 = max(0,(i-Time_lag))
        Quarterly_Returns[i]  = np.log(NAV[t2]/NAV[t1])
    #Now to sort the Quarters Series so as to only contain valid quarters
    Start_date = Quarterly_Returns.index[0].to_pydatetime()
    End_date = Quarterly_Returns.index[len(Quarterly_Returns)-1].to_pydatetime()
    Valid_quarters = []
    Valid_quarters_indices = []
    for i in range(len(Quarters)):
        if (Quarters[i].end_time.to_pydatetime())>=Start_date:
            if (Quarters[i].start_time.to_pydatetime())<=End_date:
                Valid_quarters.append(Quarters[i])
    Index = pd.Series(Valid_quarters)
    
    #Finding the average quarterly reports
    Avg_quarterly_returns = pd.Series(np.zeros(len(Index)),Index) 
    k = 0  # this variable is there to track the dates in NAV data
    Quarterly_dates = Quarterly_Returns.index
    #print len(Quarterly_dates)
    for i in range(len(Index)):
        #print ('i : ',i)
        Sum_quarterly = 0
        num = 0
        while((Quarterly_dates[k] < Index[i].start_time) or (Quarterly_dates[k]<Quarterly_dates[0])):
            k = k+1
            #print ('Quarterly_dates : ',Quarterly_dates[k])
            #print ('start_date',Index[i].start_time)
            #print ('k1 :',k)
        while (k < len(NAV)) and ((Quarterly_dates[k]<=Quarterly_Returns.index[len(Quarterly_Returns)-1]) and (Quarterly_dates[k]<=Index[i].end_time)):
            Sum_quarterly = Sum_quarterly + Quarterly_Returns[k]
            k = k+1
            #print ('k2:',k)
            num = num+1
            #print ('num :',num)
        Mean = Sum_quarterly/num
        Avg_quarterly_returns[i] = Mean
        
    #Doing zero padding of data to make downstream processing easier
    #Avg_quarterly_returns_padded = pd.Series(np.zeros(len(Quarters)),Quarters)
    #Avg_quarterly_returns_index = Avg_quarterly_returns.index
    #for i in range(len(Avg_quartery_returns_padded)):
    #    if Quarters[i] in Avg_quarterly_returns_index:
    #        Avg_quarterly_returns_padded[i] = Avg_quarterly_returns[i]
    #    else:
    #        Avg_quarterly_returns_padded[i] = 0
    print len(Avg_quarterly_returns)
    os.chdir(Write_path)
    Avg_quarterly_returns.to_csv(file)
    