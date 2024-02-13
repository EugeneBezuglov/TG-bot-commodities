# -*- coding: utf-8 -*-
"""
Created on Wed Jan 31 18:35:27 2024
https://t.me/test_bot_bezuglov_bot
@author: John
"""

import logging
import telebot
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


@bot.message_handler(commands=['start'])
def start_command(message):
   bot.send_message(
       message.chat.id,
       'Greetings! I can show you historical price data of various commodities.\n' +
       'To get help press /help.'
   )

@bot.message_handler(commands=['help'])
def help_command(message):
   keyboard = telebot.types.InlineKeyboardMarkup()
   keyboard.add(
       telebot.types.InlineKeyboardButton(
           'Message the developer', url='telegram.me/johncornish'
       )
   )
   bot.send_message(
       message.chat.id,
       '1. To receive a list of available commodities press /commodities.\n' +
       '2. Click on the commodity you are interested in.\n',
       reply_markup=keyboard
   )

@bot.message_handler(commands=['commodities'])
def exchange_command(message):
    # create an InlineKeyboardMarkup object to display inline keyboard buttons
    keyboard = telebot.types.InlineKeyboardMarkup()
    # add rows of inline keyboard buttons for each commodity
    keyboard.row(
        telebot.types.InlineKeyboardButton('Natural Gas', callback_data='Natural Gas'),
        telebot.types.InlineKeyboardButton('Brent', callback_data='Brent')
        )
    keyboard.row(
        telebot.types.InlineKeyboardButton('WTI', callback_data='WTI'),
        telebot.types.InlineKeyboardButton('Coffee', callback_data='Coffee')
        )
    keyboard.row(
        telebot.types.InlineKeyboardButton('Copper', callback_data='Copper'),
        telebot.types.InlineKeyboardButton('Aluminum', callback_data='Aluminum')
        )
    keyboard.row(
        telebot.types.InlineKeyboardButton('Cotton', callback_data='Cotton'),
        telebot.types.InlineKeyboardButton('Sugar', callback_data='Sugar')
        )
    keyboard.row(
        telebot.types.InlineKeyboardButton('Corn', callback_data='Corn'),
        telebot.types.InlineKeyboardButton('Wheat', callback_data='Wheat')
        )
    keyboard.row(
        telebot.types.InlineKeyboardButton('Global Commodities Index', callback_data='Global Commodities Index')
        )
    # send a message to the chat with the list of commodities as inline keyboard buttons
    bot.send_message(message.chat.id, 'To get the latest price, click on the commodity button:', reply_markup=keyboard)

@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    # for callback_data find the corresponding commodity in the dictionary
    for value in MessageParser().commodity_mapping.values():
        if call.data == value:
            # when the commodity is found, simulate sending a text message with the commodity name to the bot.
            simulated_message = telebot.types.Message(message_id=call.message.message_id + 1,
                        from_user=call.from_user,
                        date=call.message.date,
                        chat = call.message.chat,
                        content_type='text',
                        options=[],
                        json_string='')
            simulated_message.text = value
            # processes the message as if it was received from a user in real-time
            bot.process_new_messages([simulated_message])
           
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
    # fetch rank type and position (it's responsible for returning top/bottom N values or max/min Nth value)
    rank_type, rank_position = parser.get_price_ranking(words)

    # Send parsed variables. Comment out before deployment
    debug_operations.send_debug_message(message)
    
    # Guard Clauses
    # check if a user sends a correct product name / correct help command
    if not product:
        bot.send_message(message.from_user.id, "Я тебя не понимаю. Напиши /help.")
        return
    
    # Fetch data from the data base
    try:
        # database connection
        conn = db_operations.db_connect()
        
        # create SQL query
        sql, params = db_operations.create_sql_query(product, date_1, date_2, interval)            

        # fetch data
        df = pd.read_sql_query(sql, conn, params=params)
        
        # check if the dataframe is empty
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