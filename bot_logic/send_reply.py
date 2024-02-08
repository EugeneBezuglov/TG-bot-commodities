# -*- coding: utf-8 -*-
"""
Created on Wed Jan 31 23:04:29 2024

@author: John
"""

from bot_logic import create_reply
from bot_utils import bot_operations

# Create a bot instance
bot = bot_operations.initialize_bot()

def send_reply(df, product, date_1, date_2, interval, image_type, message):
    
    text_reply = create_reply.create_reply_string(df, product, date_1, date_2, interval, image_type)

    if text_reply:
                
        bot.send_message(message.from_user.id, text_reply)
        
    elif image_type == 'plot':
        
        create_reply.create_plot_pcot(df, product)
        # send a plot
        bot.send_photo(message.from_user.id, photo=open('plot.png', 'rb')) 
    
    else:
        # generate an image
        image_filenames = create_reply.create_table_png(df)
        
        for filename in image_filenames:
            with open(filename, 'rb') as image_file:
                bot.send_photo(message.from_user.id, photo=image_file)  
     
