# -*- coding: utf-8 -*-
"""
Created on Wed Jan 31 18:35:27 2024

@author: John
"""

import logging
import pandas as pd
from bot_logic import db_operations, send_reply, create_reply
from bot_logic.message_parser import MessageParser
from bot_utils import bot_operations, debug_operations, file_operations


# Set up logging configuration
logging.basicConfig(filename='bot.log',
                    level=logging.DEBUG,
                    format='%(asctime)s [%(levelname)s] - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

# Create a bot instance
bot = bot_operations.initialize_bot()
   
@bot.message_handler(content_types=['text'])
def get_text_messages(message):

    # Parse the message    
    # instantiate the MessageParser object
    parser = MessageParser()
    # split the message into a list of words
    words = parser.split_message(message.text)
    # fetch a product
    product = parser.get_product(words)
    # fetch dates
    date_1, date_2 = parser.find_minmax_dates(words)
    # fetch date interval (it determines whether the output of the bot is annual, monthly, or daily data)
    interval = parser.get_interval(date_1, date_2)
    # fetch image type (it determines whether a bot should reply with an image and which type of image)
    image_type = parser.get_image_type(words)

    
    # send parsed variables. Comment out before deployment
    debug_operations.send_debug_message(message)
    
    # Guard Clauses
    # check if a user sends a correct product name / correct help command
    if not product and message.text != "/help":
        bot.send_message(message.from_user.id, "Я тебя не понимаю. Напиши /help.")
        return
    if message.text == "/help":
        bot.send_message(message.from_user.id, "Напиши пшеница")
        return
    
    try:
        # database connection
        conn = db_operations.db_connect()
        
        # create SQL query
        sql, params = db_operations.create_sql_query(product, date_1, date_2, interval)            

        # fetch data
        df = pd.read_sql_query(sql, conn, params=params)
        
        print(df)
        # Check if the DataFrame is empty
        if df.empty:
            bot.send_message(message.from_user.id, "No data found for the given product/data combination.")
        
    except Exception as e:  # Catch specific exceptions
        # error handling section
        error_message = f'Error occurred: {str(e)}'
        bot.send_message(message.from_user.id, error_message)
        logging.error(error_message, exc_info=True)  # Log the error with traceback information
        
    finally:
        # close database connection
        if conn:
            conn.close()
    
    # generate and send a reply
    send_reply.send_reply(df, product, date_1, date_2, interval, image_type, message)  
    
    # remove image files (images are created when the reply is not a string)
    file_operations.delete_images(df_image_filenames = create_reply.create_table_png(df))   
    
bot.polling(none_stop=True, interval=0)