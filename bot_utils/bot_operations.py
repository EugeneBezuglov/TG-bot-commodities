# -*- coding: utf-8 -*-
"""
Created on Fri Feb  2 13:27:05 2024

@author: John
"""
import telebot # to create a bot
from decouple import Config, RepositoryEnv # to store certain parameters in a separate file (for security reasons)

def get_config():
    # Explicitly tell config where the settings file is
    config = Config(RepositoryEnv("D:\Data Analysis\DA Projects\Python Projects\TG bot commodities\config\settings.env"))
    return config

def initialize_bot():
    # Create a bot instance
    config = get_config()
    bot_token = config('STORED_TOKEN', default='')
    bot = telebot.TeleBot(bot_token)
    return bot