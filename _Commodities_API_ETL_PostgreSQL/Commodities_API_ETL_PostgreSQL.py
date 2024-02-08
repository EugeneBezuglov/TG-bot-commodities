#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# alphavantage API: ZJZMA8E2YK5H9EDE
# https://www.alphavantage.co/documentation/


# In[ ]:


import requests
import pandas as pd
import psycopg2
import psycopg2.extras as extras
from datetime import datetime
import pytz


# ## Step 1: Retrieve Data from Alphavantage APIs

# ### Natural Gas

# This API returns the Henry Hub natural gas spot prices in daily, weekly, and monthly horizons.
# 
# Source: U.S. Energy Information Administration, Henry Hub Natural Gas Spot Price, retrieved from FRED, Federal Reserve Bank of St. Louis. This data feed uses the FRED® API but is not endorsed or certified by the Federal Reserve Bank of St. Louis.
# 
# Symbol: DHHNGSP.

# In[ ]:


# Natural Gas
url = 'https://www.alphavantage.co/query?function=NATURAL_GAS&interval=daily&apikey=ZJZMA8E2YK5H9EDE'
# execute the API query
r = requests.get(url)
# collect the json data from URL\
data = r.json()

# explore the collected data
print(data.keys())


# In[ ]:


# convert json to DF
df_gas = pd.DataFrame(data)


# In[ ]:


# extract date and value from dictionary
df_gas['date'] = df_gas['data'].apply(lambda x: x.get('date'))
df_gas['value'] = df_gas['data'].apply(lambda x: x.get('value'))
df_gas.drop('data', axis=1, inplace=True)
df_gas['symbol'] = 'DHHNGSP'
df_gas['name'] = 'Natural Gas'


# In[ ]:


df_gas


# ### Crude Oil (Brent)

# This API returns the Brent (Europe) crude oil prices in daily, weekly, and monthly horizons.
# 
# Source: U.S. Energy Information Administration, Crude Oil Prices: Brent - Europe, retrieved from FRED, Federal Reserve Bank of St. Louis.
# 
# Symbol: DCOILBRENTEU.

# In[ ]:


# Crude Oil (Brent)
url = 'https://www.alphavantage.co/query?function=BRENT&interval=daily&apikey=ZJZMA8E2YK5H9EDE'
# execute the API query
r = requests.get(url)
# collect the json data from URL
data = r.json()

# explore the collected data
print(data.keys())


# In[ ]:


df_brent = pd.DataFrame(data)


# In[ ]:


# extract date and value from dictionary
df_brent['date'] = df_brent['data'].apply(lambda x: x.get('date'))
df_brent['value'] = df_brent['data'].apply(lambda x: x.get('value'))
df_brent.drop('data', axis=1, inplace=True)
df_brent['symbol'] = 'DCOILBRENTEU'
df_brent['name'] = 'Brent'


# In[ ]:


df_brent


# ### Crude Oil (WTI)

# This API returns the West Texas Intermediate (WTI) crude oil prices in daily, weekly, and monthly horizons.
# 
# Source: U.S. Energy Information Administration, Crude Oil Prices: West Texas Intermediate (WTI) - Cushing, Oklahoma, retrieved from FRED, Federal Reserve Bank of St. Louis.
# 
# Symbol: DCOILWTICO.

# In[ ]:


# Crude Oil (WTI)
url = 'https://www.alphavantage.co/query?function=WTI&interval=daily&apikey=ZJZMA8E2YK5H9EDE'
# execute the API query
r = requests.get(url)
# collect the json data from URL
data = r.json()

# explore the collected data
print(data.keys())


# In[ ]:


df_wti = pd.DataFrame(data)


# In[ ]:


# extract date and value from dictionary
df_wti['date'] = df_wti['data'].apply(lambda x: x.get('date'))
df_wti['value'] = df_wti['data'].apply(lambda x: x.get('value'))
df_wti.drop('data', axis=1, inplace=True)
df_wti['symbol'] = 'DCOILWTICO'
df_wti['name'] = 'WTI'


# In[ ]:


df_wti


# ### Copper

# This API returns the global price of copper in monthly, quarterly, and annual horizons.
# 
# Source: International Monetary Fund (IMF Terms of Use), Global price of Copper, retrieved from FRED, Federal Reserve Bank of St. Louis.
# 
# Symbol: PCOPPUSDM.

# In[ ]:


# Copper
url = 'https://www.alphavantage.co/query?function=COPPER&interval=daily&apikey=ZJZMA8E2YK5H9EDE'
# execute the API query
r = requests.get(url)
# collect the json data from URL
data = r.json()

# explore the collected data
print(data.keys())


# In[ ]:


df_copper = pd.DataFrame(data)


# In[ ]:


# extract date and value from dictionary
df_copper['date'] = df_copper['data'].apply(lambda x: x.get('date'))
df_copper['value'] = df_copper['data'].apply(lambda x: x.get('value'))
df_copper.drop('data', axis=1, inplace=True)
df_copper['symbol'] = 'PCOPPUSDM'
df_copper['name'] = 'Copper'


# In[ ]:


df_copper


# ### Aluminum

# This API returns the global price of aluminum in monthly, quarterly, and annual horizons.
# 
# Source: International Monetary Fund (IMF Terms of Use), Global price of Aluminum, retrieved from FRED, Federal Reserve Bank of St. Louis.
# 
# Symbol: PALUMUSDM.

# In[ ]:


# Aluminum
url = 'https://www.alphavantage.co/query?function=ALUMINUM&interval=daily&apikey=ZJZMA8E2YK5H9EDE'
# execute the API query
r = requests.get(url)
# collect the json data from URL
data = r.json()

# explore the collected data
print(data.keys())


# In[ ]:


df_aluminum = pd.DataFrame(data)


# In[ ]:


# extract date and value from dictionary
df_aluminum['date'] = df_aluminum['data'].apply(lambda x: x.get('date'))
df_aluminum['value'] = df_aluminum['data'].apply(lambda x: x.get('value'))
df_aluminum.drop('data', axis=1, inplace=True)
df_aluminum['symbol'] = 'PALUMUSDM'
df_aluminum['name'] = 'Aluminum'


# In[ ]:


df_aluminum


# ### Wheat

# This API returns the global price of wheat in monthly, quarterly, and annual horizons.
# 
# Source: International Monetary Fund (IMF Terms of Use), Global price of Wheat, retrieved from FRED, Federal Reserve Bank of St. Louis.
# 
# Symbol: PWHEAMTUSDM.

# In[ ]:


# Wheat
url = 'https://www.alphavantage.co/query?function=WHEAT&interval=daily&apikey=ZJZMA8E2YK5H9EDE'
# execute the API query
r = requests.get(url)
# collect the json data from URL
data = r.json()

# explore the collected data
print(data.keys())


# In[ ]:


df_wheat = pd.DataFrame(data)


# In[ ]:


# extract date and value from dictionary
df_wheat['date'] = df_wheat['data'].apply(lambda x: x.get('date'))
df_wheat['value'] = df_wheat['data'].apply(lambda x: x.get('value'))
df_wheat.drop('data', axis=1, inplace=True)
df_wheat['symbol'] = 'PWHEAMTUSDM'
df_wheat['name'] = 'Wheat'


# In[ ]:


df_wheat


# ### Corn

# This API returns the global price of corn in monthly, quarterly, and annual horizons.
# 
# Source: International Monetary Fund (IMF Terms of Use), Global price of Corn, retrieved from FRED, Federal Reserve Bank of St. Louis.
# 
# Symbol: PMAIZMTUSDM.

# In[ ]:


# Corn
url = 'https://www.alphavantage.co/query?function=CORN&interval=daily&apikey=ZJZMA8E2YK5H9EDE'
# execute the API query
r = requests.get(url)
# collect the json data from URL
data = r.json()

# explore the collected data
print(data.keys())


# In[ ]:


df_corn = pd.DataFrame(data)


# In[ ]:


# extract date and value from dictionary
df_corn['date'] = df_corn['data'].apply(lambda x: x.get('date'))
df_corn['value'] = df_corn['data'].apply(lambda x: x.get('value'))
df_corn.drop('data', axis=1, inplace=True)
df_corn['symbol'] = 'PMAIZMTUSDM'
df_corn['name'] = 'Corn'


# In[ ]:


df_corn


# ### Cotton

# This API returns the global price of cotton in monthly, quarterly, and annual horizons.
# 
# Source: International Monetary Fund (IMF Terms of Use), Global price of Cotton, retrieved from FRED, Federal Reserve Bank of St. Louis.
# 
# Symbol: PCOTTINDUSDM.

# In[ ]:


# Cotton
url = 'https://www.alphavantage.co/query?function=COTTON&interval=daily&apikey=ZJZMA8E2YK5H9EDE'
# execute the API query
r = requests.get(url)
# collect the json data from URL
data = r.json()

# explore the collected data
print(data.keys())


# In[ ]:


df_cotton = pd.DataFrame(data)


# In[ ]:


# extract date and value from dictionary
df_cotton['date'] = df_cotton['data'].apply(lambda x: x.get('date'))
df_cotton['value'] = df_cotton['data'].apply(lambda x: x.get('value'))
df_cotton.drop('data', axis=1, inplace=True)
df_cotton['symbol'] = 'PCOTTINDUSDM'
df_cotton['name'] = 'Cotton'


# In[ ]:


df_cotton


# ### Sugar

# This API returns the global price of sugar in monthly, quarterly, and annual horizons.
# 
# Source: International Monetary Fund (IMF Terms of Use), Global price of Sugar, No. 11, World, retrieved from FRED, Federal Reserve Bank of St. Louis.
# 
# Symbol: PSUGAISAUSDM.

# In[ ]:


# Sugar
url = 'https://www.alphavantage.co/query?function=SUGAR&interval=daily&apikey=ZJZMA8E2YK5H9EDE'
# execute the API query
r = requests.get(url)
# collect the json data from URL
data = r.json()

# explore the collected data
print(data.keys())


# In[ ]:


df_sugar = pd.DataFrame(data)


# In[ ]:


# extract date and value from dictionary
df_sugar['date'] = df_sugar['data'].apply(lambda x: x.get('date'))
df_sugar['value'] = df_sugar['data'].apply(lambda x: x.get('value'))
df_sugar.drop('data', axis=1, inplace=True)
df_sugar['symbol'] = 'PSUGAISAUSDM'
df_sugar['name'] = 'Sugar'


# In[ ]:


df_sugar


# ### Coffee

# This API returns the global price of coffee in monthly, quarterly, and annual horizons.
# 
# Source: International Monetary Fund (IMF Terms of Use), Global price of Coffee, Other Mild Arabica, retrieved from FRED, Federal Reserve Bank of St. Louis.
# 
# Symbol: PCOFFOTMUSDM.

# In[ ]:


# Coffee
url = 'https://www.alphavantage.co/query?function=COFFEE&interval=daily&apikey=ZJZMA8E2YK5H9EDE'
# execute the API query
r = requests.get(url)
# collect the json data from URL
data = r.json()

# explore the collected data
print(data.keys())


# In[ ]:


df_coffee = pd.DataFrame(data)


# In[ ]:


# extract date and value from dictionary
df_coffee['date'] = df_coffee['data'].apply(lambda x: x.get('date'))
df_coffee['value'] = df_coffee['data'].apply(lambda x: x.get('value'))
df_coffee.drop('data', axis=1, inplace=True)
df_coffee['symbol'] = 'PCOFFOTMUSDM'
df_coffee['name'] = 'Coffee'


# In[ ]:


df_coffee


# ### Global Commodities Index

# This API returns the global price index of all commodities in monthly, quarterly, and annual temporal dimensions.
# 
# Source: International Monetary Fund (IMF Terms of Use), Global Price Index of All Commodities, retrieved from FRED, Federal Reserve Bank of St. Louis.
# 
# Symbol: PALLFNFINDEXQ.

# In[ ]:


# Global Commodities Index
url = 'https://www.alphavantage.co/query?function=ALL_COMMODITIES&interval=daily&apikey=ZJZMA8E2YK5H9EDE'
# execute the API query
r = requests.get(url)
# collect the json data from URL
data = r.json()

# explore the collected data
print(data.keys())


# In[ ]:


df_all = pd.DataFrame(data)


# In[ ]:


# extract date and value from dictionary
df_all['date'] = df_all['data'].apply(lambda x: x.get('date'))
df_all['value'] = df_all['data'].apply(lambda x: x.get('value'))
df_all.drop('data', axis=1, inplace=True)
df_all['symbol'] = 'PALLFNFINDEXQ'
df_all['name'] = 'Global Commodities Index'


# In[ ]:


df_all


# ## Step 2: Concatenate DFs and Change Data Types

# In[ ]:


# concatenate dataframes
frames = [df_gas, df_brent, df_wti, df_copper, df_aluminum, df_wheat, df_corn, df_cotton, df_sugar, df_coffee, df_all]
df = pd.concat(frames, axis=0, ignore_index=True)


# In[ ]:


# convert data types
df['value'] = pd.to_numeric(df['value'], errors='coerce')
df['date'] = pd.to_datetime(df['date'], errors='coerce')


# In[ ]:


# drop missing values
df.dropna(inplace=True)


# In[ ]:


df


# In[ ]:


df.dtypes


# ## Step 3: Connect to PostgreSQL Database

# In[ ]:


try:
    # try to connect to the DB
    conn = psycopg2.connect(dbname='commodities', user='postgres', password='11112222', host='localhost')
    print('Connected successfully')
except:
    # if connections fails
    print('Can`t establish connection to database')


# In[ ]:


def upsert_values(conn, df, table):

    # create a list of tuples where each tuple contains every single line value from the df
    # Essentially, each tuple = each line of values
    tuples = [tuple(x) for x in df.to_numpy()]
    cols = ','.join(list(df.columns))

    # the %%s variable accepts the list of tuples created above, 'do nothing' ignores conflicts and only inserts new records:
    query = 'INSERT INTO %s(%s) VALUES %%s ON CONFLICT DO NOTHING' % (table, cols)
    # a cursor is needed to allow submission of SQL queries to the connection
    cursor = conn.cursor()
    
    # get Moscow time zone for log file
    logtime = datetime.now(pytz.timezone('Europe/Moscow'))

    try:
        # execute the SQL code using the cursor
        extras.execute_values(cursor, query, tuples)
        # commit the query to the database
        conn.commit()
        message = 'PostgreSQL 15 — commodities DB — commodities Table has been successfully updated at %s' %logtime
    except (Exception, psycopg2.DatabaseError) as error:
        message = 'Error: %s' %error
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()
    # write log
    with open('D:\Data Analysis\logs\commodities_log.txt', 'a') as log:
        log.write('\n%s\n%s' %(logtime, message))
        log.close()


# In[ ]:


table = 'commodities'
upsert_values(conn, df, table)

