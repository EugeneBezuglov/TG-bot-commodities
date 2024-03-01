import logging
import telebot
import pandas as pd
from bot_logic import db_operations, send_reply, create_reply
from bot_logic.message_parser import MessageParser
from bot_utils import bot_operations, debug_operations, file_operations
import matplotlib.pyplot as plt
import seaborn as sns

#%%

product = 'WTI'
date_1 = '2004-01'
date_2 = '2024-02'
interval = 'monthly'
rank_type = None
rank_position = None


# Fetch data from the data base
try:
    # database connection
    conn = db_operations.db_connect()
    
    # create SQL query
    sql, params = db_operations.create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)            

    # fetch data
    df_price = pd.read_sql_query(sql, conn, params=params)
   
except Exception as e:  # Catch specific exceptions
    # error handling section
    error_message = f'Error occurred: {str(e)}'
    print(error_message)
    
finally:
    # close database connection
    if conn:
        conn.close()


#%%
# Fetch Google trends data
try:
    # database connection
    conn = db_operations.db_connect()
    
    # create SQL query
    fields = "date, wti AS trend"
    table = "trends"
    order = "date ASC"
    sql_trends = (
        f"SELECT {fields} "
        f"FROM {table} "
        f"ORDER BY {order};"
        )
    
    # fetch data
    df_trends = pd.read_sql_query(sql_trends, conn)
except Exception as e:  # Catch specific exceptions
    # error handling section
    error_message = f'Error occurred: {str(e)}'
    print(error_message)    
    
finally:
    # close database connection
    if conn:
        conn.close()    

        
#%% df_price

df_price = df_price.drop(columns=['unit'])
max_price = df_price['price'].max()
df_price['rel_price'] = round((df_price['price'] / max_price) * 100, 0).astype(int)
#df_price['rel_price'] = df_price['rel_price'].astype(int)
df_price['price_change'] = abs(df_price['price'].pct_change()*100)



#%%

df_price['date'] = pd.to_datetime(df_price['date'])
df_price['date'] = df_price['date'].dt.strftime('%Y-%m')

df_trends['date'] = pd.to_datetime(df_trends['date'])
df_trends['date'] = df_trends['date'].dt.strftime('%Y-%m')
df_trends['trend_change'] = abs(df_trends['trend'].pct_change()*100)

#%%




#%%
df = pd.merge(df_price, df_trends, on='date')

#%%
# Plot rel_price
plt.plot(df['date'], df['rel_price'], label='Relative Price')

# Plot brent_trend
plt.plot(df['date'], df['trend'], label='Trend')

# Customize plot
plt.title('Relative Price and Google Trend Over Time')
plt.xlabel('Date')
plt.ylabel('Value')
plt.legend()
plt.grid(True)

# Show plot
plt.show()
#%%
df['rel_price'].corr(df['trend'])
#%%

df.corr(method='spearman')



#%%
filtered_df = df[df['date'] > '2012-01']
correlation = filtered_df['rel_price'].corr(filtered_df['trend'], method='spearman')
print(correlation)
#%%
import phik
#%%

df.phik_matrix()

#%%

df[df['date'] > '2012-01'].phik_matrix()

#%%
import numpy as np

#%%
df['trend_change'] = df['trend_change'].replace([np.inf, -np.inf], np.nan)
df = df.dropna()

#%%

# Calculate phi_k correlation matrix again
phi_k_corr_matrix = df.phik_matrix()

# Check the correlation between 'trend_change' and 'price_change'
print(phi_k_corr_matrix['trend_change']['price_change'])









