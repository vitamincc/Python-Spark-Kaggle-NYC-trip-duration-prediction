#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Oct 28 02:11:25 2017

@author: xucc
"""


import csv 
from math import sqrt
import datetime
from collections import defaultdict
from math import radians, sin, cos, sqrt, asin
import pandas as pd

#calculate distance between start point to end 
def haversine(lat1, lon1, lat2, lon2):
    
    R = 6372.8/1.6
    
    dLat = radians(lat2 - lat1)
    dLon = radians(lon2 - lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)
    
    a = sin(dLat/2)**2 + cos(lat1)*cos(lat2)*sin(dLon/2)**2
    c = 2*asin(sqrt(a))
    
    return R*c



#%%
#read train data
train_taxi = defaultdict(list)
with open('/Users/xucc/Documents/GMU/CS657/assigns/assign3/train_taxi3.csv', 'r') as file:
    csv_reader = csv.reader(file)
    csv_reader.next()
    for row in csv_reader:
        if float(row[10]) > 60:
        #calculate distance between start point to end
            dist = round(haversine(float(row[5]), float(row[6]), float(row[7]), float(row[8])),3)
             #select distance between 0~100 miles and driving time > 60s
            if dist > 0 and dist < 50:
                train_taxi[row[0]] = [row[1],row[4]]
                train_taxi[row[0]].append(dist)
        
                #record the pickup time
                start = datetime.datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S')
                sHour = round(start.hour/24.0, 2)
                sMinu = round(start.minute/60.0, 2)
                sSec = round(start.minute/60.0, 2)
            
                train_taxi[row[0]].append(sHour)
                train_taxi[row[0]].append(sMinu)
                train_taxi[row[0]].append(sSec)
        
                #append observed value
                train_taxi[row[0]].append(row[10])
        

train_data = pd.DataFrame.from_dict(train_taxi, orient = "index")

#normalize first~third rows: taxi company id, number of passangers, miles
for i in range(3):
    train_data[i] = train_data[i].astype(float)
    if i == 0:
        train_data[i] = train_data[i] / 2.0
    if i == 1:
        train_data[i] = train_data[i] / 10
    if i == 2:
        train_data[i] = train_data[i] / 50

        
train_data.columns = ['taxiID','nPass','Dist','Hour','Minute','Second','Duration']
train_data.to_csv('/Users/xucc/Documents/GMU/CS657/assigns/assign3/train3.csv')


'''
#%%
#read test data
test_taxi = defaultdict(list)
with open('/Users/xucc/Documents/GMU/CS657/assigns/assign3/test_taxi15.csv', 'r') as file:
    csv_reader = csv.reader(file)
    csv_reader.next()
    for row in csv_reader:
        if not row:
            continue
        
        dist = haversine(float(row[4]), float(row[5]), float(row[6]), float(row[7]))
        if dist > 0 and dist < 100:
            test_taxi[row[0]] = [row[1],row[3]]
            test_taxi[row[0]].append(dist)
        
            #record the pickup time
            start = datetime.datetime.strptime(row[2], '%Y-%m-%d %H:%M:%S')
            sHour = round(start.hour/24.0, 2)
            sMinu = round(start.minute/60.0, 2)
            sSec = round(start.minute/60.0, 2)
        
            test_taxi[row[0]].append(sHour)
            test_taxi[row[0]].append(sMinu)
            test_taxi[row[0]].append(sSec)
            test_taxi[row[0]].append(0)
        
test_data = pd.DataFrame.from_dict(test_taxi, orient = "index")

#%%
for i in range(3):
    test_data[i] = test_data[i].astype(float)
    if i == 0:
        test_data[i] = test_data[i] / 2.0
    if i == 1:
        test_data[i] = test_data[i] / 10
    if i == 2:
        test_data[i] = test_data[i] / 50
 #%%       

test_data.columns = ['vendorID','nPass','Dist','Hour','Minute','Second','Duration']
test_data.to_csv('/Users/xucc/Documents/GMU/CS657/assigns/assign3/test1.csv')
'''