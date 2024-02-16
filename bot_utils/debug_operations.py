# -*- coding: utf-8 -*-
"""
Created on Wed Jan 31 20:28:25 2024

@author: John
"""
from bot_utils import bot_operations
from bot_logic.message_parser import MessageParser

# Create a bot instance
bot = bot_operations.initialize_bot()

def send_debug_message(message):
        
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
    
    bot.send_message(message.from_user.id,
                     f"```\n"
                     f"product: {product}\n"
                     f"date_1: {date_1}\n"
                     f"date_2: {date_2}\n"
                     f"interval: {interval}\n"
                     f"image_type: {image_type}\n"
                     f"rank_type: {rank_type}\n"
                     f"rank_position: {rank_position}\n"
                     f"```", parse_mode='Markdown')    
