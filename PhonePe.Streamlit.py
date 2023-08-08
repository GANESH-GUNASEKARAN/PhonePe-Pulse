#!/usr/bin/env python
# coding: utf-8


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
import streamlit as st



host = 'xxxx'
port = 'yyyy'
database = 'zzzz'
username = 'aaaa'
password = 'your password'
PROJECT = psycopg2.connect(host=host, port=port, database=database, user=username, password=password)
cursor = PROJECT.cursor()



cursor.execute('''select * from aggtrans;''')
PROJECT.commit()
T1=cursor.fetchall()
aggtrans=pd.DataFrame(T1, columns=['State', 'Year', 'Quarter', 'TransactionType', 'TransactionCount', 'TransactionAmount'])


cursor.execute('''select * from aggusers;''')
PROJECT.commit()
T2=cursor.fetchall()
aggusers=pd.DataFrame(T2, columns=['State', 'Year', 'Quarter', 'Brands', 'TransactionCount', 'Percentage'])



cursor.execute('''select * from maptrans;''')
PROJECT.commit()
T3=cursor.fetchall()
maptrans=pd.DataFrame(T3, columns=['State', 'Year', 'Quarter', 'Count', 'Amount'])



cursor.execute('''select * from mapusers;''')
PROJECT.commit()
T4=cursor.fetchall()
mapusers=pd.DataFrame(T4, columns=['State', 'Year', 'Quarter', 'Users', 'AppOpens'])



cursor.execute('''select * from toptrans;''')
PROJECT.commit()
T5=cursor.fetchall()
toptrans=pd.DataFrame(T5, columns=['State', 'Year', 'Quarter', 'Count', 'Amount' ])



cursor.execute('''select * from topusers;''')
PROJECT.commit()
T6=cursor.fetchall()
topusers=pd.DataFrame(T6, columns=['State', 'Year', 'Quarter', 'Pincodes', 'RegisteredUsers'] )

PROJECT.rollback()

#QUERIES:

def one():
    AG1=aggtrans[['State','TransactionCount']]
    count=AG1.groupby('State')['TransactionCount'].sum()
    TC1=pd.DataFrame(count).reset_index()
    TC1=TC1.sort_values(by='TransactionCount', ascending=False)
    TC1=TC1.head(10)
    fig=px.bar(TC1,x='State',y='TransactionCount',color_discrete_sequence=px.colors.sequential.gray)
    st.plotly_chart(fig)


def two():
    AG2=aggtrans[['State','TransactionCount']]
    count=AG2.groupby('State')['TransactionCount'].sum()
    TC2=pd.DataFrame(count).reset_index()
    TC2=TC2.sort_values(by='TransactionCount', ascending=True)
    TC2=TC2.head(10)
    fig=px.bar(TC2,x='State',y='TransactionCount',color_discrete_sequence=px.colors.sequential.gray)
    st.plotly_chart(fig)


def three():
    AG3=aggtrans[['State','TransactionAmount']]
    count=AG3.groupby('State')['TransactionAmount'].sum()
    TA1=pd.DataFrame(count).reset_index()
    TA1=TA1.sort_values(by='TransactionAmount', ascending=False)
    TA1=TA1.head(10)
    fig=px.bar(TA1,x='State',y='TransactionAmount',color_discrete_sequence=px.colors.sequential.gray)
    st.plotly_chart(fig)


def four():
    AG4=aggtrans[['State','TransactionAmount']]
    count=AG4.groupby('State')['TransactionAmount'].sum()
    TA2=pd.DataFrame(count).reset_index()
    TA2=TA2.sort_values(by='TransactionAmount', ascending=True)
    TA2=TA2.head(10)
    fig=px.bar(TA2,x='State',y='TransactionAmount',color_discrete_sequence=px.colors.sequential.gray)
    st.plotly_chart(fig)
    

def five():
    AU1=aggusers[['Brands','TransactionCount']]
    count=AU1.groupby('Brands')['TransactionCount'].sum()
    B1=pd.DataFrame(count).reset_index()
    B1=B1.sort_values(by='TransactionCount', ascending=True)
    B1=B1.head(10)
    fig=px.bar(B1,x='Brands',y='TransactionCount',color_discrete_sequence=px.colors.sequential.gray)
    st.plotly_chart(fig)


def six():
    MT1=maptrans[['Year','Amount']]
    count=MT1.groupby('Year')['Amount'].sum()
    D1=pd.DataFrame(count).reset_index()
    D1=D1.sort_values(by='Amount', ascending=False)
    D1=D1.head(10)
    fig=px.bar(D1,x='Year',y='Amount',color_discrete_sequence=px.colors.sequential.gray)
    st.plotly_chart(fig)


def seven():
    MU1=mapusers[['State','Users']]
    count=MU1.groupby('State')['Users'].sum()
    U1=pd.DataFrame(count).reset_index()
    U1=U1.sort_values(by='Users', ascending=False)
    U1=U1.head(10)
    fig=px.bar(U1,x='State',y='Users',color_discrete_sequence=px.colors.sequential.gray)
    st.plotly_chart(fig)


def eight():
    MU2=mapusers[['State','AppOpens']]
    count=MU2.groupby('State')['AppOpens'].sum()
    U2=pd.DataFrame(count).reset_index()
    U2=U2.sort_values(by='AppOpens', ascending=True)
    U2=U2.head(10)
    fig=px.bar(U2,x='State',y='AppOpens',color_discrete_sequence=px.colors.sequential.gray)
    st.plotly_chart(fig)


def nine():
    topusers["Pincodes"] = topusers["Pincodes"].astype(str)
    TU1=topusers[['Pincodes','RegisteredUsers']]
    count=TU1.groupby('Pincodes')['RegisteredUsers'].sum()
    P1=pd.DataFrame(count).reset_index()
    P1=P1.sort_values(by='RegisteredUsers', ascending=False)
    P1=P1.head(10)
    fig=px.bar(P1,x='Pincodes',y='RegisteredUsers',color_discrete_sequence=px.colors.sequential.gray)
    st.plotly_chart(fig)


def ten():
    AU2=aggusers[['Brands','TransactionCount']]
    count=AU2.groupby('Brands')['TransactionCount'].sum()
    B2=pd.DataFrame(count).reset_index()
    B2=B2.sort_values(by='TransactionCount', ascending=False)
    B2=B2.head(10)
    fig=px.bar(B2,x='Brands',y='TransactionCount',color_discrete_sequence=px.colors.sequential.gray)
    st.plotly_chart(fig)


# MAP:

def t(year,qr):
    yr=int(year)
    q=int(qr)
    AT=aggtrans[['State','Year','Quarter', 'TransactionCount']]
    AT1=AT.loc[(AT['Year']==yr)& (AT['Quarter']==q)]
    AT2=AT1[['State','TransactionCount']]
    AT2=AT2.sort_values('State')

    url="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response = requests.get(url)
    data=json.loads(response.content)
    state_names_trans=[feature['properties']['ST_NM'] for feature in data['features']]
    state_names_trans.sort()
    df_state_names_trans=pd.DataFrame({'State':state_names_trans})

    AT2['State'] = AT2['State'].str.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar')
    AT2['State'] = AT2['State'].str.replace('---',' & ')
    AT2['State'] = AT2['State'].str.replace('-',' ')
    AT2['State'] = AT2['State'].str.replace('Dadra & Nagar Haveli & Daman & Diu','Dadra and Nagar Haveli and Daman and Diu')
    AT2['State'] = AT2['State'].str.title()

    merge_df=df_state_names_trans.merge(AT2, on='State')

    #merge_df.to_csv('State_trans.csv', index=False)
    #df_trans=pd.read_csv('State_trans.csv')
    trans_fig = px.choropleth(merge_df,
                geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                              featureidkey='properties.ST_NM', locations='State',color='TransactionCount', color_continuous_scale='gray', title='TRANSACTIONCOUNT')

    trans_fig.update_geos(fitbounds="locations",visible=False)
    trans_fig.update_layout(title_font=dict(size=33),title_font_color='#6739b7')
    st.plotly_chart(trans_fig)


# STREAMLIT

st.title("PHONEPE DATA VISUALISATION AND EXPLORATION")
tab1,tab2=st.tabs(['ONE','TWO'])
with tab1:
    col1,col2=st.columns(2)
    with col1:
        tr_year = st.selectbox('**Select Year**', ('2018','2019','2020','2021','2022'))
    with col2:
        tr_quarter = st.selectbox('**Select Quarter**',('1','2','3','4'))
    t(tr_year,tr_quarter)
with tab2:
    Q=st.selectbox('**Select Query**', ('States with high transaction count','States with least transaction count','States with high transaction amount','States with least transaction amount','Phone brands with least transaction count','Years with transaction amount','States with most users','States with most app opens','Pincodes with most registered users','Phone brands with most transaction count'))
    
    if Q=='States with high transaction count':
        one()
    elif Q=='States with least transaction count':
        two()
    elif Q=='States with high transaction amount':
        three()
    elif Q=='States with least transaction amount':
        four()
    elif Q=='Phone brands with least transaction count':
        five()  
    elif Q=='Years with transaction amount':
        six()      
    elif Q=='States with most users':
        seven()              
    elif Q=='States with most app opens':
        eight()
    elif Q=='Pincodes with most registered users':
        nine()
    elif Q=='Phone brands with most transaction count':
        ten()







