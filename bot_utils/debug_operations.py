# -*- coding: utf-8 -*-
"""
Created on Wed Jan 31 20:28:25 2024

@author: John
"""
from bot_logic import parse_message as pm
from bot_utils import bot_operations

# Create a bot instance
bot = bot_operations.initialize_bot()

def send_debug_message(message):
    product, date_1, date_2, interval, image_type = pm.parse_message(message.text)
    bot.send_message(message.from_user.id,
                     f"```\n"
                     f"product: {product}\n"
                     f"date_1: {date_1}\n"
                     f"date_2: {date_2}\n"
                     f"interval: {interval}\n"
                     f"image_type: {image_type}\n"
                     f"```", parse_mode='Markdown')    
