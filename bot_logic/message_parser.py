# -*- coding: utf-8 -*-
"""
Created on Mon Feb  5 11:25:04 2024

@author: John
"""
import re

class MessageParser:
    def __init__(self):
        # Create tuples for each of the commodities to cover typos
        self.gas = ('газ', 'гас', 'газз', 'ггаз', 'gas', 'natural gas', 'Natural Gas', 'Gas')
        self.brent = ('брент', 'brent', 'нефть','бренд', 'brent', 'oil', 'brent oil', 'Brent')
        self.wti = ('wti', 'нефть wti', 'WTI', 'wti oil')
        self.coffee = ('coffee', 'кофе', 'Coffee', 'Кофе')
        self.copper = ('copper', 'медь', 'Copper')
        self.aluminum = ('aluminum', 'алюминий', 'Aluminum')
        self.cotton = ('cotton', 'хлопок', 'Cotton')
        self.sugar = ('sugar', 'сахар', 'Sugar')
        self.corn = ('corn', 'кукуруза', 'Corn')
        self.wheat = ('пшеница', 'пшенница', 'пшница', 'Пшеница', 'wheat', 'Wheat', 'зерно')
        self.global_commodities_index = ('global index', 'global', 'index', 'gci', 'GCI', 'глобальный индекс', 'индекс', 'Global Commodities Index', 'Index')
        
        # Use these tuples as keys
        self.commodity_mapping = {
            self.gas: 'Natural Gas',
            self.brent: 'Brent',
            self.wti: 'WTI',
            self.coffee: 'Coffee',
            self.copper: 'Copper',
            self.aluminum: 'Aluminum',
            self.cotton: 'Cotton',
            self.sugar: 'Sugar',
            self.corn: 'Corn',
            self.wheat: 'Wheat',
            self.global_commodities_index: 'Global Commodities Index'
        }
        
        # Tuples for image types
        self.plot = ('график', 'граф', 'г', 'plot', 'graph', 'line')
        self.table = ('таблица', 'табл', 'т', 'table')
        
        # Use these tuples as keys
        self.image_type_mapping = {
            self.plot: 'plot',
            self.table: 'table'
        }


    def split_message(self,string):
        # replace certain chars and convert a string to a list of words
        words = re.sub("[;:,.]"," ", string).split()
        return words
    
    def find_dates(self,strings_list):
        # Create regex pattern to capture dates
        regex_pattern = (
            r'\b(?:19|20)\d{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])\b'
            r'|'
            r'\b(?:19|20)\d{2}-(?:0[1-9]|1[0-2])\b'
            r'|'
            r'\b(?:19|20)\d{2}\b'
        )
      
        # find all potential dates in the list of words
        dates_list = re.findall(regex_pattern, ' '.join(strings_list))
        return dates_list
     
    def find_minmax_dates(self,strings_list):
        """
        Extracts the earliest and latest dates from a list of strings.
    
        Args:
            strings_list (list): A list of strings containing potential date representations.
    
        Returns:
            tuple: A tuple containing the earliest and latest dates found in the strings_list.
                   The dates are represented as strings in the formats 'YYYY-MM-DD', 'YYYY-MM', or 'YYYY'.
    
                   If no dates are found, both elements of the tuple will be empty strings.
    
        Example:
            Given strings_list = ['Meeting on 2023-01-15', 'Deadline is 2022-12-31', 'Report due by 2024'],
            the function would return ('2022-12-31', '2024').
        """        
        # Create regex pattern to capture dates
        regex_pattern = (
            r'\b(?:19|20)\d{2}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])\b'
            r'|'
            r'\b(?:19|20)\d{2}-(?:0[1-9]|1[0-2])\b'
            r'|'
            r'\b(?:19|20)\d{2}\b'
        )   
        
        # find all potential dates in the list of strings
        dates = re.findall(regex_pattern, ' '.join(strings_list))
        
        # Sort the list of dates
        sorted_dates = sorted(dates)
       
        #Extract the earliest date
        min_date = sorted_dates[0] if dates else ''
        
        # Extract the latest date
        max_date = sorted_dates[-1] if len(sorted_dates) > 1 and sorted_dates[-1] != sorted_dates[0] else ''
        
        return min_date, max_date
    
    def get_product(self,strings_list):
        """
        Extracts a single product from a list of strings based on commodity mappings.

        Args:
            strings_list (list): A list of strings potentially containing references to products.

        Returns:
            str: A single product extracted from the strings_list based on commodity mappings.
                 If multiple products are found, or no product is found, an empty string is returned.

        Algorithm:
            1. Count the number of products mentioned in the strings_list.
            2. If only one product is mentioned, extract that product.
            3. If more than one product is mentioned, or no product is mentioned, return an empty string.
        """
        # count the number of products mentioned in the strings_list
        n_products = sum(1 for string in strings_list if any(string in commodity_tuple for commodity_tuple in self.commodity_mapping.keys()))
        # if only one product is mentioned
        if n_products == 1:
            # extract the product
            product = next((self.commodity_mapping[commodity_tuple] for commodity_tuple in self.commodity_mapping.keys() if any(string in commodity_tuple for string in strings_list)), '')
        else:
            product = None
        return product

    def get_interval(self, date_1, date_2):
        # get the minimum length of the dates.
        if date_1 and date_2:
            min_len = min(len(date_1), len(date_2))
            # assign the interval based of the min_len value
            interval = 'annually' if min_len == 4 else 'monthly' if min_len == 7 else 'daily' if min_len == 10 else None 
        elif date_1 and not date_2:
            interval = 'annually' if len(date_1) == 4 else 'monthly' if len(date_1) == 7 else 'daily' if len(date_1) == 10 else None
        else:
            interval = None
        
        return interval        


    def get_image_type(self,strings_list):
        """
        Extracts an image type from a list of strings based on image type mappings.

        Args:
            strings_list (list): A list of strings potentially containing references to image types.

        Returns:
            str: A single image type extracted from the strings_list based on image type mappings.
                 If multiple image types are found, or no image type is found, an empty string is returned.

        Algorithm:
            1. Count the number of image types mentioned in the strings_list.
            2. If only one image type is mentioned, extract it.
            3. If more than one image type is mentioned, or no image type is mentioned, return an empty string.
        """
        # count the number of image types mentioned in the strings_list
        n_types = sum(1 for string in strings_list if any(string in image_type_tuple for image_type_tuple in self.image_type_mapping.keys()))
        # if only one product is mentioned
        if n_types == 1:
            # extract the image type
            image_type = next((self.image_type_mapping[image_type_tuple] for image_type_tuple in self.image_type_mapping.keys() if any(string in image_type_tuple for string in strings_list)), '')
        else:
            image_type = None
        return image_type
    
    
    def get_price_ranking(self, strings_list):
        # top N -> return a table of N highest prices and corresponding dates/periods
        # bottom N -> return a table of N lowest prices and corresponding dates/periods
        # max N -> return an Nth highest price value and the corresponding date/period
        # mix N ->return an Nth lowest price value and the corresponding date/period
        rank_type = None
        rank_type_count = 0
        rank_position = None
        valid_keywords = {'top', 'bottom', 'max', 'min'}
        
        # find the rank type. If multiple types, return none
        for string in strings_list:
            if string in valid_keywords:
                rank_type = string
                rank_type_count += 1
                if rank_type_count > 1:
                    rank_type = None
                    rank_position = None
                    break

        # if a valid rank type is found, check for a rank position
        if rank_type is not None:
            # Find the index of the rank_type in the strings_list
            rank_type_index = strings_list.index(rank_type)    
            # Check the next string after the rank_type for a valid rank position
            if rank_type_index + 1 < len(strings_list):
                next_string = strings_list[rank_type_index + 1]
                if next_string.isdigit() and 1 <= int(next_string) <= 10:
                    rank_position = int(next_string)
        
        return rank_type, rank_position