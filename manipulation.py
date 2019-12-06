import json
import pandas as pd

def xmltojson(xmlfile,jsonfile):
    datasource = open(xmlfile)
    file=datasource.read()
    data=json.dumps(xmltodict.parse(file),indent=4)
    with open(jsonfile, 'w') as outfile:
        outfile.writelines(data)
#xmltojson('dinesafe.xml','dinesafe.json')

def clean_dinesafe():
    df=pd.read_json('dinesafe.json')
    df=df.drop(['ROW_ID','COURT_OUTCOME','AMOUNT_FINED','MINIMUM_INSPECTIONS_PERYEAR','ESTABLISHMENT_STATUS'],axis=1)
    df['SEVERITY']=df['SEVERITY'].fillna(0)
    df['INFRACTION_DETAILS']=df['INFRACTION_DETAILS'].fillna('non')
    df['ACTION']=df['ACTION'].fillna('non')
    #print(df['SEVERITY'].value_counts())
    df=df.replace({'SEVERITY': {'M - Minor':1,'S - Significant':2,'NA - Not Applicable':3,'C - Crucial':5}})
    #print(df['SEVERITY'].value_counts())
    df['name']=df['ESTABLISHMENT_NAME'].str.lower()
    df['name']=df['name'].str.replace('[^\w\s]','')
    print(df.info())
    data=df.to_json(orient='records')
    with open('toronto.json', 'w') as outfile:
        outfile.writelines(data)    
#clean_dinesafe()

def select_T_from_yelp():
    df=pd.read_json('business.json',lines=True)
    df=df.loc[df['city']=='Toronto']
    df=df.loc[df['categories'].str.contains('estaurant', regex=False)==True]
    df=df.drop(['hours','is_open','state'],axis=1)
    df=df.dropna()
    data=df.to_json(orient='records')
    with open('trbus.json', 'w') as outfile:
        outfile.writelines(data)    
    print(df.info())
    #return df
#select_T_from_yelp()

def join_text():
    df=pd.read_json('tip.json',lines=True)
    df=df.drop(['user_id','compliment_count'],axis=1)
    df=df[df['date']>'2017-10-01']
    df=df.drop(['date'],axis=1)
    df=df.groupby(['business_id'])['text'].apply(lambda x: '|'.join(x)).reset_index()
    df['text']=df['text'].fillna('non')
    print(df.head())
    data=df.to_json(orient='records')
    with open('trtip.json', 'w') as outfile:
        outfile.writelines(data)    
    print(df.info())
    #return df
#join_text()

def merge_bus_text():
    bus=pd.read_json('trbus.json')
    tip=pd.read_json('trtip.json')
    df=bus.merge(tip,how='left',on='business_id')
    df['name']=df['name'].str.lower()
    df['name']=df['name'].str.replace('[^\w\s]','')
    print(bus.info(),tip.info(),df.info())
    print(df.head())
    data=df.to_json(orient='records')
    with open('yelp.json', 'w') as outfile:
        outfile.writelines(data)
    #return df
#merge_bus_text()
    

def merge_yelp_toronto():
    yelp=pd.read_json('yelp.json')
    toronto=pd.read_json('toronto.json')
    toronto['LONGITUDE']=toronto['LONGITUDE'].round(2)
    toronto['LATITUDE']=toronto['LATITUDE'].round(2)
    yelp['longitude']=yelp['longitude'].round(2)
    yelp['latitude']=yelp['latitude'].round(2)
    df=toronto.merge(yelp,how='left',left_on=['name','LONGITUDE','LATITUDE'],right_on=['name','longitude','latitude'])
    print(yelp.info(),toronto.info(),df.info())
    df=df.dropna()
    df=df.drop(['LONGITUDE','LATITUDE'],axis=1)
    print(df.info())
    #print(df.head())
    data=df.to_json(orient='records')
    with open('yelp_toronto.json', 'w') as outfile:
        outfile.writelines(data)
#merge_yelp_toronto()


def clean_la():
    df=pd.read_csv('la.csv')
    df=df[df['City']=='Las Vegas']
    df['name']=df['Location_Name'].str.lower()
    df['name']=df['name'].str.replace('[^\w\s]','')
    df['Date_Current']=df['Date_Current'].map(lambda x:x[:10])
    df['Inspection_Date']=df['Inspection_Date'].map(lambda x:x[:10])
    df['Inspection_Time']=df['Inspection_Time'].map(lambda x:str(str(x).split('T')[1:])[2:10])
    #df['Inspection_Time']=df['Inspection_Time'].map(lambda x:x[10:18])
    df['latitude']=df['Location_1'].map(lambda x:float(x.split(',')[0][1:])).round(1)
    df['longitude']=df['Location_1'].map(lambda x:-float(x.split(',')[1][:-1])).round(1)
    df=df.drop(['City','State','Permit_Status','Location_1','Inspection_Grade','Zip','ObjectId','Restaurant_Name','Record_Updated','Address','Employee_ID'],axis=1)
    #print(df.info())
    return df
    
def clean_bus():
    df=pd.read_json('business.json',lines=True)
    
    df=df[df['city']=='Las Vegas']
    df['name']=df['name'].str.lower()
    df['name']=df['name'].str.replace('[^\w\s]','')
    df['latitude']=df['latitude'].round(1)
    df['longitude']=df['longitude'].round(1)
    data=df.to_json(orient='records')
    with open('busla.json', 'w') as outfile:
        outfile.writelines(data)
        
#clean_bus()
#la=clean_la()

bus=pd.read_json('busla.json')
def merge_la_bus():
    df=la.merge(bus,how='left',left_on=['name','longitude','latitude'],right_on=['name','longitude','latitude'])
    print(df.info())
    print(df.head())
    df=df.dropna(subset=['business_id'])
    df['temp']=df['Inspection_Result'].map({'\'A\' Grade': 100, '\'B\' Downgrade': 90,'\'C\' Downgrade': 80,
                                             'Closed without Fees':70 ,'Closed with Fees':60})
    df['Grade']=df['temp']-df['Inspection_Demerits']
    df=df.drop(['temp'],axis=1)
    df=df[['business_id','Category_Name','stars','review_count','attributes','categories','Grade']]
    data=df.to_json(orient='records')
    with open('Inspection_La.json', 'w') as outfile:
        outfile.writelines(data)
    #df.to_csv('new.csv')
    return df
import numpy as np

#m=merge_la_bus()

df=pd.read_json('business.json',lines=True)
df.info()
'''
lst=list(la.columns)
for c in lst:
    print(c,len(la[c].unique()))
    



m1=m.categories.astype(str).str.strip(',').str.get_dummies(',')
lst1=list(m1.sum())
lst2=list(m1.columns)#lst 2 stop words
lst3=list(zip(lst1,lst2))
lst4=[a[1] for a in lst3 if a[0]<30]
m2=m1.drop(lst4,axis=1)
'''
