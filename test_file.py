import logging
import telebot
import pandas as pd
from bot_logic import db_operations, send_reply, create_reply
from bot_logic.message_parser import MessageParser
from bot_utils import bot_operations, debug_operations, file_operations
import matplotlib.pyplot as plt
import seaborn as sns



product = 'Brent'
date_1 = '1900-01'
date_2 = '2023-12'
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
    df = pd.read_sql_query(sql, conn, params=params)
    

    
except Exception as e:  # Catch specific exceptions
    # error handling section
    error_message = f'Error occurred: {str(e)}'
    print(error_message)
    
finally:
    # close database connection
    if conn:
        conn.close()
        


# Set the style of the plot
sns.set(style="whitegrid")

# Plot histogram
sns.histplot(data=df, x='price', bins=15)

# Add labels and title
plt.xlabel('Price')
plt.ylabel('Frequency')
plt.title('Histogram of Price Data')
plt.tight_layout()

# Show plot
plt.show()