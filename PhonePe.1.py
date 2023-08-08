#!/usr/bin/env python
# coding: utf-8

# In[ ]:





# In[38]:


import pandas as pd
import json
import os
import plotly.express as px 
import requests
import subprocess
import plotly.graph_objects as go
from IPython.display import display
import psycopg2
import numpy as np


# In[39]:


path1="data/aggregated/transaction/country/india/state"
Agg_state_list=os.listdir(path1)
clm1={'State':[], 'Year':[],'Quarter':[],'TransactionType':[], 'TransactionCount':[], 'TransactionAmount':[]}
for i in Agg_state_list:
    p_i=path1+"/"+i
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+"/"+j
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
          p_k=p_j+"/"+k
          Data=open(p_k, 'r')
          A = json.load(Data)
          for g in A['data']['transactionData']:
            Name=g['name']
            count=g['paymentInstruments'][0]['count']
            amount=g['paymentInstruments'][0]['amount']
            clm1['TransactionType'].append(Name)
            clm1['TransactionCount'].append(count)
            clm1['TransactionAmount'].append(amount)
            clm1['State'].append(i)
            clm1['Year'].append(j)
            clm1['Quarter'].append(int(k.strip('.json')))
#Succesfully created a dataframe
Agg_Trans=pd.DataFrame(clm1)
Agg_Trans['State'] = Agg_Trans['State'].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
Agg_Trans['State'] = Agg_Trans['State'].str.replace('---',' & ')
Agg_Trans['State'] = Agg_Trans['State'].str.replace('-',' ')
Agg_Trans['State'] = Agg_Trans['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

Agg_Trans['State'] = Agg_Trans['State'].str.title()




# In[40]:


path2="data/aggregated/user/country/india/state/"
user_list = os.listdir(path2)
clm2 = {'State': [], 'Year': [], 'Quarter': [], 'Brands': [], 'TransactionCount': [],'Percentage': []}
for i in user_list:
    p_i = path2 + "/"+ i
    Agg_yr = os.listdir(p_i)

    for j in Agg_yr:
        p_j = p_i + "/"+ j
        Agg_yr_list = os.listdir(p_j)
        for k in Agg_yr_list:
          p_k = os.path.join(p_j, k)
          if os.path.isfile(p_k):# Check if the file exists before attempting to open it
              with open(p_k, 'r') as Data:
                B = json.load(Data)
                try:
                  for a in B['data']['usersByDevice']:
                    Brand=a['brand']
                    Count=a['count']
                    Percentage=a['percentage']
                    clm2['Brands'].append(Brand)
                    clm2['TransactionCount'].append(Count)
                    clm2['Percentage'].append(Percentage)
                    clm2['State'].append(i)
                    clm2['Year'].append(j)
                    clm2['Quarter'].append(int(k.strip('.json')))
                except:
                  pass
Agg_Users=pd.DataFrame(clm2)

Agg_Users['State'] = Agg_Users['State'].str.replace('---', ' & ')
Agg_Users['State'] = Agg_Users['State'].str.replace('-', ' ')
Agg_Users['State'] = Agg_Users['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')
Agg_Users['State'] = Agg_Users['State'].str.replace('Andaman & Nicobar Islands', 'Andaman & Nicobar')
Agg_Users['State'] = Agg_Users['State'].str.title()


# In[41]:


path3="data/map/transaction/hover/country/india/state"
Agg_state_list=os.listdir(path3)
clm3={'State':[], 'Year':[],'Quarter':[], 'District':[], 'Count':[], 'Amount':[]}
for i in Agg_state_list:
    p_i=path3+"/"+i
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+"/"+j
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
          p_k=p_j+"/"+k
          Data=open(p_k, 'r')
          C = json.load(Data)
          for n in C['data']['hoverDataList']:
                District=n['name']
                Count=n['metric'][0]['count']
                Amount=n['metric'][0]['amount']
                clm3['District'].append(District)
                clm3['Count'].append(Count)
                clm3['Amount'].append(Amount)
                clm3['State'].append(i)
                clm3['Year'].append(j)
                clm3['Quarter'].append(int(k.strip('.json')))
                
Map_Trans=pd.DataFrame(clm3)

Map_Trans['State'] = Map_Trans['State'].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
Map_Trans['State'] = Map_Trans['State'].str.replace('---',' & ')
Map_Trans['State'] = Map_Trans['State'].str.replace('-',' ')
Map_Trans['State'] = Map_Trans['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

Map_Trans['State'] = Map_Trans['State'].str.title()


                


# In[42]:


path4="data/map/user/hover/country/india/state"
Agg_state_list=os.listdir(path4)
clm4={'State':[], 'Year':[],'Quarter':[], 'District':[], 'Users':[], 'AppOpens':[]}
for i in Agg_state_list:
    p_i=path4+"/"+i
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+"/"+j
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
          p_k=p_j+"/"+k
          Data=open(p_k, 'r')
          D = json.load(Data)
          for e in D['data']['hoverData'].items():
                District=e[0]
                Users=e[1]['registeredUsers']
                AppOpens=e[1]['appOpens']
                clm4['District'].append(District)
                clm4['Users'].append(Users)
                clm4['AppOpens'].append(AppOpens)
                clm4['State'].append(i)
                clm4['Year'].append(j)
                clm4['Quarter'].append(int(k.strip('.json')))
                                
Map_Users=pd.DataFrame(clm4)
                
    
Map_Users['State'] = Map_Users['State'].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
Map_Users['State'] = Map_Users['State'].str.replace('---',' & ')
Map_Users['State'] = Map_Users['State'].str.replace('-',' ')
Map_Users['State'] = Map_Users['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

Map_Users['State'] = Map_Users['State'].str.title()

    


# In[43]:


path5="data/top/transaction/country/india/state"
Agg_state_list=os.listdir(path5)
clm5={'State':[], 'Year':[],'Quarter':[], 'District':[], 'Count':[], 'Amount':[]}
for i in Agg_state_list:
    p_i=path5+"/"+i
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+"/"+j
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
          p_k=p_j+"/"+k
          Data=open(p_k, 'r')
          E = json.load(Data)
          for s in E['data']['districts']:
                District=s['entityName']
                Count=s['metric']['count']
                Amount=s['metric']['amount']
                clm5['District'].append(District)
                clm5['Count'].append(Count)
                clm5['Amount'].append(Amount)
                clm5['State'].append(i)
                clm5['Year'].append(j)
                clm5['Quarter'].append(int(k.strip('.json')))
                
                
                
                
Top_Trans=pd.DataFrame(clm5)

Top_Trans['State'] = Top_Trans['State'].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
Top_Trans['State'] = Top_Trans['State'].str.replace('---',' & ')
Top_Trans['State'] = Top_Trans['State'].str.replace('-',' ')
Top_Trans['State'] = Top_Trans['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

Top_Trans['State'] = Top_Trans['State'].str.title()
           


# In[44]:


path6="data/top/user/country/india/state"
Agg_state_list=os.listdir(path6)
clm6={'State':[], 'Year':[],'Quarter':[], 'Pincodes':[], 'RegisteredUsers':[]}
for i in Agg_state_list:
    p_i=path6+"/"+i
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+"/"+j
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
          p_k=p_j+"/"+k
          Data=open(p_k, 'r')
          F = json.load(Data)
          for h in F['data']['pincodes']:
                Pincodes=h['name']
                RegisteredUsers=h['registeredUsers']
                clm6['Pincodes'].append(Pincodes)
                clm6['RegisteredUsers'].append(RegisteredUsers)
                clm6['State'].append(i)
                clm6['Year'].append(j)
                clm6['Quarter'].append(int(k.strip('.json')))
                
                
Top_Users=pd.DataFrame(clm6)
                
    
Top_Users['State'] = Top_Users['State'].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
Top_Users['State'] = Top_Users['State'].str.replace('---',' & ')
Top_Users['State'] = Top_Users['State'].str.replace('-',' ')
Top_Users['State'] = Top_Users['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')

Top_Users['State'] = Top_Users['State'].str.title()


# In[45]:


host = 'localhost'
port = '5432'
database = 'project_1'
username = 'postgres'
password = 'ganeshgunasekaran'
PROJECT = psycopg2.connect(host=host, port=port, database=database, user=username, password=password)
cursor = PROJECT.cursor()


# In[47]:


#path1
cursor.execute('''create table if not exists AggTrans(State varchar(50), 
                       Year int, 
                       Quarter int, 
                       TransactionType varchar(50), 
                       TransactionCount bigint, 
                       TransactionAmount bigint)''') 
                       
PROJECT.commit()
for _, row in Agg_Trans.iterrows():
            insert_query = ''' 
                 INSERT INTO  AggTrans (State, Year, Quarter, TransactionType, TransactionCount, TransactionAmount)
                 VALUES (%s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['State'],
                row['Year'],
                row['Quarter'],
                row['TransactionType'],
                row['TransactionCount'],
                row['TransactionAmount'],
            )
            cursor.execute(insert_query, values)
PROJECT.commit()


# In[21]:


PROJECT.rollback()


# In[59]:


#path2
cursor.execute('''create table if not exists AggUsers(State varchar(50), 
                       Year int, 
                       Quarter int, 
                       Brands varchar(50), 
                       TransactionCount bigint, 
                       Percentage float)''') 
                       
PROJECT.commit()
for _, row in Agg_Users.iterrows():
            insert_query = ''' 
                 INSERT INTO  AggUsers (State, Year, Quarter, Brands, TransactionCount, Percentage )
                 VALUES (%s, %s, %s, %s, %s, %s)

            '''
            values = (
                row['State'],
                row['Year'],
                row['Quarter'],
                row['Brands'],
                row['TransactionCount'],
                row['Percentage']
            )
            cursor.execute(insert_query, values)
PROJECT.commit()


# In[58]:


PROJECT.rollback()


# In[51]:


#path3
cursor.execute('''create table if not exists MapTrans(State varchar(50), 
                       Year int, 
                       Quarter int, 
                       Count bigint, 
                       Amount bigint)''') 
                       
PROJECT.commit()
for _, row in Map_Trans.iterrows():
            insert_query = ''' 
                 INSERT INTO  MapTrans (State, Year, Quarter, Count, Amount)
                 VALUES (%s, %s, %s, %s, %s)

            '''
            values = (
                row['State'],
                row['Year'],
                row['Quarter'],
                row['Count'],
                row['Amount'],
            )
            cursor.execute(insert_query, values)
PROJECT.commit()


# In[50]:


PROJECT.rollback()


# In[52]:


#path4
cursor.execute('''create table if not exists MapUsers(State varchar(50), 
                       Year int, 
                       Quarter int, 
                       Users bigint, 
                       AppOpens bigint)''')  
                        
                       
PROJECT.commit()
for _, row in Map_Users.iterrows():
            insert_query = ''' 
                 INSERT INTO  MapUsers (State, Year, Quarter, Users, AppOpens)
                 VALUES (%s, %s, %s, %s, %s)

            '''
            values = (
                row['State'],
                row['Year'],
                row['Quarter'],
                row['Users'],
                row['AppOpens'],
            )
            cursor.execute(insert_query, values)
PROJECT.commit()


# In[53]:


#path5
cursor.execute('''create table if not exists TopTrans(State varchar(50), 
                       Year int, 
                       Quarter int,  
                       Count bigint, 
                       Amount bigint)''') 
                       
PROJECT.commit()
for _, row in Top_Trans.iterrows():
            insert_query = ''' 
                 INSERT INTO  TopTrans (State, Year, Quarter, Count, Amount)
                 VALUES (%s, %s, %s, %s, %s)

            '''
            values = (
                row['State'],
                row['Year'],
                row['Quarter'],
                row['Count'],
                row['Amount'],
            )
            cursor.execute(insert_query, values)
PROJECT.commit()


# In[54]:


#path6
cursor.execute('''create table if not exists TopUsers(State varchar(50), 
                       Year int, 
                       Quarter int, 
                       Pincodes varchar(50), 
                       RegisteredUsers bigint)''') 
                       
PROJECT.commit()
for _, row in Top_Users.iterrows():
            insert_query = ''' 
                 INSERT INTO  TopUsers (State, Year, Quarter, Pincodes, RegisteredUsers)
                 VALUES (%s, %s, %s, %s, %s)

            '''
            values = (
                row['State'],
                row['Year'],
                row['Quarter'],
                row['Pincodes'],
                row['RegisteredUsers'],
            )
            cursor.execute(insert_query, values)
PROJECT.commit()


# In[68]:





# In[ ]:




