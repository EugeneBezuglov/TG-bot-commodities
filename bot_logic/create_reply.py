# -*- coding: utf-8 -*-
"""
Created on Wed Jan 31 19:49:22 2024

@author: John
"""
import dataframe_image as dfi # to convert df into image
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib.dates import DateFormatter
import pandas as pd


def create_plot_pcot(df, product):
    '''
    Create a line plot of price change over time
    
    Parameters:
    - df (DataFrame): The DataFrame containing the data with columns 'date', 'price', and 'unit'.
    - product (str): The name of the product for which the price change plot is to be created.
    '''
    # Get item of measurement for the price label
    price_unit = df['unit'].iloc[0]
    # Convert 'date' column to datetime format
    df['date'] = pd.to_datetime(df['date'])
    
    # Determine the date format for x-axis labels based on the df['date'] data
    # It makes sure yearly data is labeled with YYYY, monthly with YYYY-MM, and daily with YYYY-MM-DD
    date_format = ("%Y" if (df['date'].dt.strftime('%m-%d') == '01-01').all()
                   else "%Y-%m" if (df['date'].dt.strftime('%d') == '01').all()
                   else "%Y-%m-%d")
      
    # Plotting
    #plt.figure(figsize=(6, 4))
    plot = sns.lineplot(x='date', y='price', data=df, ci=None)
    
    # Format x-axis with the specified date format
    myFmt = DateFormatter(date_format)  # Customize the date format as needed
    plot.xaxis.set_major_formatter(myFmt)

    # Set plot title, xlabel, and ylabel
    plot.set(title=f'{product} Price Change Over Time',
             xlabel=None,
             ylabel=f'Price ({price_unit})')
    plt.xticks(rotation=45, ha='right')
    # Adjust layout for better visualization (otherwise dates would be cropped)
    plt.tight_layout()
    # Save the plot as a PNG image
    plt.savefig('plot.png')
    # Clear the current figure to prevent multiple plots in a single run
    plt.clf()

def create_table_png(df):
    '''
    Turn a dataframe into an image
    50 rows max
    '''
    #dfi.export(df,'table.png', max_rows=100)

    chunk_size = 30
    num_chunks = (len(df) // chunk_size) + 1
    df_image_filenames = []
    
    for i in range(num_chunks):
        start_idx = i * chunk_size
        end_idx  =(i + 1) * chunk_size
        chunk_df = df.iloc[start_idx:end_idx]
        
        filename = f'table_chunk_{i + 1}.png'
        dfi.export(chunk_df, filename)
        df_image_filenames.append(filename)
    
    return df_image_filenames

# Create reply string
def create_reply_string(df, product, date_1, date_2, interval, image_type, rank_type, rank_position):
    
    """
    Create a reply string based on the provided parameters.
​
    Parameters:
    - df (pandas.DataFrame): The DataFrame containing the data.
    - product (str): The name of the product.
    - date_1 (str): The start date.
    - date_2 (str): The end date.
    - interval (str): The time interval for the data (e.g., 'annually').
    - image_type (str): The type of image ('plot', 'table', etc.).
​
    Returns:
    - (str): The generated reply string.
    """
    def get_suffix(rank_position):
        if rank_position == 1:
            suffix = 'st'
        elif rank_position == 2:
            suffix = 'nd'
        elif rank_position == 3:
            suffix = 'rd'
        else:
            suffix = 'th'
        return suffix
    
    name = str(df['name'][0])
    price = str(df['price'][0])
    unit = str(df['unit'][0])

    if date_1 and not date_2 and rank_type:
        reply_str= None
        return reply_str

    # guard clause, return type shouldn't be a string
    if date_2 and not rank_type:
        reply_str = None
        return reply_str
    # guard clause, return type shouldn't be a string
    if date_2 and rank_type and rank_position:
        reply_str = None
        return reply_str
    
    if date_2 and rank_type and not rank_position:
        if rank_type == 'top' or rank_type == 'max':
            reply_str = (name+' ('+date_1+' - '+date_2+'), highest '+interval+' price: '+price
                         +', date/period: '+str(df['date'][0])
                         +', unit: '+unit
                        )
        elif rank_type == 'bottom' or rank_type == 'min':
            reply_str = (name+' ('+date_1+' - '+date_2+'), lowest '+interval+' price: '+price
                         +', date/period: '+str(df['date'][0])
                         +', unit: '+unit
                        )
    
    # if a user sends only the name of a product, return the latest available price.
    # e.g. 'brent'
    if not date_1 and not rank_type:
        
        reply_str = (name+' (latest price): '+price
                     +', date/period: '+str(df['date'][0])
                     +', unit: '+unit
                    )
        return reply_str
    # if a user sends only the name of a product and rank type, return the corresponding price.
    # e.g. 'brent max' or 'brent min'
    if not date_1 and rank_type and not rank_position:
        if rank_type == 'top' or rank_type == 'max':
            reply_str = (name+' (highest price)'+': '+price
                         +', date/period: '+str(df['date'][0])
                         +', unit: '+unit
                        )
        elif rank_type == 'bottom' or rank_type == 'min':
            reply_str = (name+' (lowest price)'+': '+price
                         +', date/period: '+str(df['date'][0])
                         +', unit: '+unit
                        )            
        return reply_str
    # if a user sends only the name of a product, rank type and rank_position, return the correspoding price(s)
    # e.g. 'brent max 3' -> return the 3rd highest price; 'brent min 2' -> return the 2nd lowest prices
    if not date_1 and rank_type and rank_position:
        suffix = get_suffix(rank_position)
        if rank_type == 'max':
            reply_str = (name+f' ({rank_position}{suffix} highest price)'+': '+price
                         +', date/period: '+str(df['date'][0])
                         +', unit: '+unit
                        )
        if rank_type == 'min':
            reply_str = (name+f' ({rank_position}{suffix} lowest price)'+': '+price
                         +', date/period: '+str(df['date'][0])
                         +', unit: '+unit
                        )
        
        # if a user sends, say, 'brent top 3', this would require an image reply, hence empty reply_str
        if rank_type in ['top', 'bottom']:
            reply_str = None
         
        return reply_str

    # if one date, return the price for the date.
    if date_1 and not date_2 and not rank_type and not rank_position:
    
        if interval == 'annually':
            reply_str = (name+' (avg. annual price)'+': '+price
                         +', period: '+date_1
                         +', unit: '+unit
                        )
        elif interval == 'monthly':
            reply_str = (name+' (avg. monthly price)'+': '+price
                         +', period: '+date_1
                         +', unit: '+unit
                        )
        elif interval == 'daily':
            reply_str = (name+': '+price
                         +', date: '+date_1
                         +', unit: '+unit
                        )
        else:
            reply_str = None
  
    return reply_str