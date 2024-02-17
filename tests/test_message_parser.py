# -*- coding: utf-8 -*-
"""
Created on Sat Feb 17 12:54:18 2024

This is a test module for MessageParser class.

@author: John
"""

from bot_logic.message_parser import MessageParser

def test_split_message():
    # check if the split_message function correctly splits a string into a list of words, removing certain characters
    parser = MessageParser()
    input_string = '2020s asd ;,. brent : 2020-11-05 2023-11-05 max'
    result = parser.split_message(input_string)
    expected_result = ['2020s', 'asd', 'brent', '2020-11-05', '2023-11-05', 'max']
    assert result == expected_result
    
    
def test_find_dates():
    parser = MessageParser()
    input_list = ['2020s', 'asd', 'brent', '2020-11-05', '2023-11-05', 'max', '2012-11', '2099-01',
                  '2020-13-01', '2020-13-35', '19999', '1999', '19992000top']
    result = parser.find_dates(input_list)
    expected_result = ['2020-11-05', '2023-11-05', '2012-11', '2099-01', '2020', '2020', '1999']
    assert result == expected_result


def test_find_minmax_dates():
    parser = MessageParser()
    input_list = ['2020s', 'asd', 'brent', '2020-11-05', '2023-11-05', 'max', '2012-11', '2099-01',
                  '2020-13-01', '2020-13-35', '19999', '1999', '19992000top']
    result = parser.find_minmax_dates(input_list)
    expected_result = ('1999', '2099-01')
    assert result == expected_result


def test_get_product_case1():
    parser = MessageParser()
    # case when user inputs 1 product
    input_list = ['2020s', 'asd', 'brent', '2020-11-05', '2023-11-05', 'max', '2012-11', '2099-01',
                  '2020-13-01', '2020-13-35', '19999', '1999', '19992000top']
    result = parser.get_product(input_list)
    expected_result = 'Brent'
    assert result == expected_result
    
def test_get_product_case2():
    parser = MessageParser()
    # case when user inputs more than 1 product
    input_list = ['2020s', 'asd', 'brent', '2020-11-05', 'gas', '2023-11-05', 'max', '2012-11', '2099-01',
                  '2020-13-01', '2020-13-35', '19999', '1999', '19992000top']
    result = parser.get_product(input_list)
    expected_result = None
    assert result == expected_result

def test_get_product_case3():
    parser = MessageParser()
    # case when user inputs 0 products
    input_list = ['2020s', 'asd', '2020-11-05', '2023-11-05', 'max', '2012-11', '2099-01',
                  '2020-13-01', '2020-13-35', '19999', '1999', '19992000top']
    result = parser.get_product(input_list)
    expected_result = None
    assert result == expected_result

def test_get_interval_case1():
    parser = MessageParser()
    # case when user inputs 1 product
    date_1 = '2000'
    date_2 = '2000-10-01'
    result = parser.get_interval(date_1, date_2)
    expected_result = 'annually'
    assert result == expected_result

def test_get_interval_case2():
    parser = MessageParser()
    # case when user inputs 1 product
    date_1 = '2000-10-01'
    date_2 = '2000'
    result = parser.get_interval(date_1, date_2)
    expected_result = 'annually'
    assert result == expected_result

def test_get_interval_case3():
    parser = MessageParser()
    # case when user inputs 1 product
    date_1 = '2000-10-01'
    date_2 = '2000-10'
    result = parser.get_interval(date_1, date_2)
    expected_result = 'monthly'
    assert result == expected_result

def test_get_interval_case4():
    parser = MessageParser()
    # case when user inputs 1 product
    date_1 = '2000-10-01'
    date_2 = '2000-11-01'
    result = parser.get_interval(date_1, date_2)
    expected_result = 'daily'
    assert result == expected_result

def test_get_interval_case5():
    parser = MessageParser()
    # case when user inputs 1 product
    date_1 = '2000-10-01'
    date_2 = ''
    result = parser.get_interval(date_1, date_2)
    expected_result = 'daily'
    assert result == expected_result

def test_get_interval_case6():
    parser = MessageParser()
    # case when user inputs 1 product
    date_1 = '2000-10'
    date_2 = ''
    result = parser.get_interval(date_1, date_2)
    expected_result = 'monthly'
    assert result == expected_result
    
def test_get_interval_case7():
    parser = MessageParser()
    # case when user inputs 1 product
    date_1 = '2000'
    date_2 = ''
    result = parser.get_interval(date_1, date_2)
    expected_result = 'annually'
    assert result == expected_result

def test_get_image_type_case1():
    # case when user inputs 1 image type
    parser = MessageParser()
    input_list = ['2020s', 'asd', '2020-11-05', '2023-11-05', 'max', '2012-11', '2099-01',
                  '2020-13-01', '2020-13-35', '19999', '1999', '19992000top', 'plot']
    result = parser.get_image_type(input_list)
    expected_result = 'plot'
    assert result == expected_result

def test_get_image_type_case2():
    # case when user inputs > 1 image types
    parser = MessageParser()
    input_list = ['2020s', 'asd', '2020-11-05', '2023-11-05', 'max', '2012-11', '2099-01',
                  '2020-13-01', '2020-13-35', '19999', '1999', 'plot', '19992000top', 'plot']
    result = parser.get_image_type(input_list)
    expected_result = None
    assert result == expected_result

def test_get_image_type_case3():
    # case when user inputs 0 image types
    parser = MessageParser()
    input_list = ['2020s', 'asd', '2020-11-05', '2023-11-05', 'max', '2012-11', '2099-01',
                  '2020-13-01', '2020-13-35', '19999', '1999', '19992000top']
    result = parser.get_image_type(input_list)
    expected_result = None
    assert result == expected_result

def test_get_price_ranking_case1():
    # case when user inputs 'max' as ranking type and no rank_position
    parser = MessageParser()
    input_list = ['2020s', 'asd', '2020-11-05', '2023-11-05', 'max', '2012-11', '2099-01',
                  '2020-13-01', '2020-13-35', '19999', '1999', '19992000top']
    result = parser.get_price_ranking(input_list)
    expected_result = ('max', None)
    assert result == expected_result

def test_get_price_ranking_case2():
    # case when user inputs 'max' and '3' as ranking type and rank_position
    parser = MessageParser()
    input_list = ['2020s', 'asd', '2020-11-05', '2023-11-05', 'max', '3', '2012-11', '2099-01',
                  '2020-13-01', '2020-13-35', '19999', '1999', '19992000top']
    result = parser.get_price_ranking(input_list)
    expected_result = ('max', 3)
    assert result == expected_result

def test_get_price_ranking_case3():
    # case when user inputs 'min' as ranking type and no rank_position
    parser = MessageParser()
    input_list = ['2020s', 'asd', '2020-11-05', '2023-11-05', 'min', '2012-11', '2099-01',
                  '2020-13-01', '2020-13-35', '19999', '1999', '19992000top']
    result = parser.get_price_ranking(input_list)
    expected_result = ('min', None)
    assert result == expected_result

def test_get_price_ranking_case4():
    # case when user inputs 'min' and '3' as ranking type and rank_position
    parser = MessageParser()
    input_list = ['2020s', 'asd', '2020-11-05', '2023-11-05', 'min', '3', '2012-11', '2099-01',
                  '2020-13-01', '2020-13-35', '19999', '1999', '19992000top']
    result = parser.get_price_ranking(input_list)
    expected_result = ('min', 3)
    assert result == expected_result

def test_get_price_ranking_case5():
    # case when user inputs 'top' as ranking type and no rank_position
    parser = MessageParser()
    input_list = ['2020s', 'asd', '2020-11-05', '2023-11-05', 'top', '2012-11', '2099-01',
                  '2020-13-01', '2020-13-35', '19999', '1999', '19992000top']
    result = parser.get_price_ranking(input_list)
    expected_result = ('top', None)
    assert result == expected_result

def test_get_price_ranking_case6():
    # case when user inputs 'top' and '3' as ranking type and rank_position
    parser = MessageParser()
    input_list = ['2020s', 'asd', '2020-11-05', '2023-11-05', 'top', '3', '2012-11', '2099-01',
                  '2020-13-01', '2020-13-35', '19999', '1999', '19992000top']
    result = parser.get_price_ranking(input_list)
    expected_result = ('top', 3)
    assert result == expected_result

def test_get_price_ranking_case7():
    # case when user inputs 'bottom' as ranking type and no rank_position
    parser = MessageParser()
    input_list = ['2020s', 'asd', '2020-11-05', '2023-11-05', 'bottom', '2012-11', '2099-01',
                  '2020-13-01', '2020-13-35', '19999', '1999', '19992000top']
    result = parser.get_price_ranking(input_list)
    expected_result = ('bottom', None)
    assert result == expected_result

def test_get_price_ranking_case8():
    # case when user inputs 'bottom' and '3' as ranking type and rank_position
    parser = MessageParser()
    input_list = ['2020s', 'asd', '2020-11-05', '2023-11-05', 'bottom', '3', '2012-11', '2099-01',
                  '2020-13-01', '2020-13-35', '19999', '1999', '19992000top']
    result = parser.get_price_ranking(input_list)
    expected_result = ('bottom', 3)
    assert result == expected_result

def test_get_price_ranking_case9():
    # case when user inputs multiple ranking types (regardless of rank_positions)
    parser = MessageParser()
    input_list = ['2020s', 'asd', 'max', '2020-11-05', '2023-11-05', 'bottom', '3', '2012-11', '2099-01',
                  '2020-13-01', '2020-13-35', '19999', '1999', '19992000top']
    result = parser.get_price_ranking(input_list)
    expected_result = (None, None)
    assert result == expected_result


















