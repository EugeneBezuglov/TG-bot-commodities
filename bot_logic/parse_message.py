# -*- coding: utf-8 -*-
"""
Created on Wed Jan 31 19:25:11 2024

@author: John
"""

import re # to use RegExp pattern

# Tuples for each of the commodities to cover typos
gas = ('газ', 'гас', 'газз', 'ггаз', 'gas', 'natural gas')
brent = ('брент', 'brent', 'нефть','бренд', 'brent', 'oil', 'brent oil')
wti = ('wti', 'нефть wti', 'WTI', 'wti oil')
coffee = ('coffee', 'кофе', 'Coffee', 'Кофе')
copper = ('copper', 'медь')
aluminum = ('aluminum', 'алюминий')
cotton = ('cotton', 'хлопок')
sugar = ('sugar', 'сахар')
corn = ('corn', 'кукуруза')
wheat = ('пшеница', 'пшенница', 'пшница', 'Пшеница', 'wheat', 'Wheat', 'зерно')
global_commodities_index = ('global index', 'global', 'index', 'gci', 'GCI', 'глобальный индекс', 'индекс')


# Use these tuples as keys
commodity_mapping = {
    gas: 'Natural Gas',
    brent: 'Brent',
    wti: 'WTI',
    coffee: 'Coffee',
    copper: 'Copper',
    aluminum: 'Aluminum',
    cotton: 'Cotton',
    sugar: 'Sugar',
    corn: 'Corn',
    wheat: 'Wheat',
    global_commodities_index: 'Global Commodities Index'
}

# Tuples for image types
plot = ('график', 'граф', 'г', 'plot', 'graph', 'line')
table = ('таблица', 'табл', 'т', 'table')

# Use these tuples as keys
image_type_mapping = {
    plot: 'plot',
    table: 'table'
}

# Get interval for data (annually, monthly, daily)
def get_interval(date):
    
    interval = ''
    
    if len(date) == 4: # Check if date is in 'YYYY' format
        interval = 'annually'
    elif len(date) == 7: # Check if date is in 'YYYY-MM' format
        interval = 'monthly'
    elif len(date) == 10:
        interval = 'daily'
    elif date == '':
        interval = 'no interval'
    else:
        interval = ''
    return interval

def parse_message(text):
    
    #
    message = re.sub("[;:,.]"," ", text).split()
    
    # Fetch dates
    # Find all dates in a specific format in the message
    dates = re.findall(r'\b\d{4}-\d{2}-\d{2}\b|\b\d{4}-\d{2}\b|\b\d{4}\b', ' '.join(message))
    dates.sort()
    #Extract the earliest date
    date_1 = dates[0] if dates else ''
    # Extract the latest date
    date_2 = dates[-1] if len(dates) > 1 and dates[-1] != dates[0] else ''

    # Fetch a commodity
    # Count the number of commodities in the message
    count_in_commodity_mapping = sum(1 for word in message if any(word in commodity_mapping for commodity_mapping in commodity_mapping.keys()))

    # Get interval
    interval = get_interval(date_1)
        
    # Assign product based on the count
    if count_in_commodity_mapping == 1:
        product = next((commodity_mapping[product] for product in commodity_mapping.keys() if any(word in product for word in message)), '')
    else:
        product = ''
        
    # Fetch an image type (plot or table; the function return an image of a table, too)
    # Count the number of image_types in the message
    count_in_image_types = sum(1 for word in message if any(word in image_type for image_type in image_type_mapping.keys()))

    # Assign image_type based on the count
    if count_in_image_types == 1:
        image_type = next((image_type_mapping[type_] for type_ in image_type_mapping.keys() if any(word in type_ for word in message)), '')
    else:
        image_type = ''
        
    return product, date_1, date_2, interval, image_type