# -*- coding: utf-8 -*-
"""
Created on Sun Feb 18 22:04:46 2024

@author: John
"""

import pytest
import psycopg2
import pandas as pd
from sqlalchemy.engine.base import Connection
from bot_logic.db_operations import db_connect
from bot_logic.db_operations import db_connect_psycopg2
from bot_logic.db_operations import create_sql_query

# Define a fixture using pytest.fixture decorator.
# Fixtures are used to provide a fixed baseline upon which tests can reliably and repeatedly execute.
# In this case, the fixture "mock_config" will provide a mocked configuration function for testing purposes.
@pytest.fixture
def mock_config(monkeypatch):
    # Define a mock configuration function.
    # This function will replace the original configuration function during testing.
    def mock_config_function(key, default):
        # Mocking the database credentials for testing purposes.
        if key == 'DB_NAME':
            return 'commodities_test'
        elif key == 'DB_LOGIN':
            return 'postgres'
        elif key == 'DB_PASSWORD':
            return '11112222'
        elif key == 'DB_HOST':
            return 'localhost'
        else:
            return default

    # Monkeypatching: Set the configuration function in the db_operations module to the mock_config_function.
    # This ensures that when db_connect is called during testing, it will use the mocked configuration.
    monkeypatch.setattr('bot_logic.db_operations.config', mock_config_function)

# The test function takes the mock_config fixture as an argument, indicating that it will use the mocked configuration.
def test_db_connect(mock_config):
    # Call the db_connect function.
    # Since the configuration function is mocked, it will use the mocked credentials for database connection.
    conn = db_connect()
    # Assert that the returned object is an instance of sqlalchemy.engine.base.Connection.
    # This ensures that the db_connect function successfully established a database connection.
    assert isinstance(conn, Connection)
    # Check if the connection is open (closed attribute is 0), indicating a successful connection.
    assert conn.closed == 0  

    conn.close()


# The test function takes the mock_config fixture as an argument, indicating that it will use the mocked configuration.
def test_db_connect_psycopg2(mock_config):
    # Call the db_connect function.
    # Since the configuration function is mocked, it will use the mocked credentials for database connection.
    conn = db_connect_psycopg2()
    # Assert that the returned object is an instance of psycopg2.extensions.connection.
    # This ensures that the db_connect function successfully established a database connection.
    assert isinstance(conn, psycopg2.extensions.connection)
    # Check if the connection is open (closed attribute is 0), indicating a successful connection.
    assert conn.closed == 0  

    conn.close()

#if product and not date_1:
#    if not rank_type:

def test_create_sql_query_not_date_1_and_not_rank_type():
    # Define input parameters
    product = 'Brent'
    date_1 = None
    date_2 = None
    interval = None
    rank_type = None
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, value AS price, date, interval, unit, symbol "
        "FROM commodities "
        "WHERE name = %s "
        "ORDER BY date DESC LIMIT 1;"
    )

    # Define the expected parameters
    expected_params = ('Brent',)
    
    data = {'name': ['Brent'], 
        'price': [79.3], 
        'date': ['2024-02-05'], 
        'interval': ['daily'], 
        'unit': ['dollars per barrel'], 
        'symbol': ['DCOILBRENTEU']}
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

#if product and not date_1:
#    if not rank_type:
#       ...
#    elif rank_type and not rank_position:

def test_create_sql_query_not_date_1_and_rank_type_top_and_not_rank_position():
    # Define input parameters
    product = 'Brent'
    date_1 = None
    date_2 = None
    interval = None
    rank_type = "top"
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, value AS price, date, interval, unit, symbol "
        "FROM commodities "
        "WHERE name = %s "
        "ORDER BY price DESC LIMIT 1;"
    )

    # Define the expected parameters
    expected_params = ('Brent',)
    
    data = {'name': ['Brent'], 
        'price': [143.95], 
        'date': ['2008-07-03'], 
        'interval': ['daily'], 
        'unit': ['dollars per barrel'], 
        'symbol': ['DCOILBRENTEU']}
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()
    
def test_create_sql_query_not_date_1_and_rank_type_bottom_and_not_rank_position():
    # Define input parameters
    product = 'Brent'
    date_1 = None
    date_2 = None
    interval = None
    rank_type = "bottom"
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, value AS price, date, interval, unit, symbol "
        "FROM commodities "
        "WHERE name = %s "
        "ORDER BY price ASC LIMIT 1;"
    )

    # Define the expected parameters
    expected_params = ('Brent',)
    
    data = {'name': ['Brent'], 
        'price': [9.1], 
        'date': ['1998-12-10'], 
        'interval': ['daily'], 
        'unit': ['dollars per barrel'], 
        'symbol': ['DCOILBRENTEU']}
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()
    
def test_create_sql_query_not_date_1_and_rank_type_max_and_not_rank_position():
    # Define input parameters
    product = 'Brent'
    date_1 = None
    date_2 = None
    interval = None
    rank_type = "max"
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, value AS price, date, interval, unit, symbol "
        "FROM commodities "
        "WHERE name = %s "
        "ORDER BY price DESC LIMIT 1;"
    )

    # Define the expected parameters
    expected_params = ('Brent',)
    
    data = {'name': ['Brent'], 
        'price': [143.95], 
        'date': ['2008-07-03'], 
        'interval': ['daily'], 
        'unit': ['dollars per barrel'], 
        'symbol': ['DCOILBRENTEU']}
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_not_date_1_and_rank_type_min_and_not_rank_position():
    # Define input parameters
    product = 'Brent'
    date_1 = None
    date_2 = None
    interval = None
    rank_type = "min"
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, value AS price, date, interval, unit, symbol "
        "FROM commodities "
        "WHERE name = %s "
        "ORDER BY price ASC LIMIT 1;"
    )

    # Define the expected parameters
    expected_params = ('Brent',)
    
    data = {'name': ['Brent'], 
        'price': [9.1], 
        'date': ['1998-12-10'], 
        'interval': ['daily'], 
        'unit': ['dollars per barrel'], 
        'symbol': ['DCOILBRENTEU']}
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

#if product and not date_1:
#    if not rank_type:
#        ...
#    elif rank_type and not rank_position:
#        ...
#    elif rank_type and rank_position:
    
def test_create_sql_query_not_date_1_and_rank_type_top_and_rank_position_3():
    # Define input parameters
    product = 'Brent'
    date_1 = None
    date_2 = None
    interval = None
    rank_type = "top"
    rank_position = 3

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = ("WITH ranked AS ("
                          "SELECT name, value AS price, date, interval, unit, symbol, DENSE_RANK() OVER(ORDER BY value DESC) as rnk "
                          "FROM commodities "
                          "WHERE name = %s ) "
                          "SELECT * FROM ranked WHERE rnk <= %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', 3)
    
    data = {'name': ['Brent', 'Brent', 'Brent'], 
        'price': [143.95, 143.68, 142.43], 
        'date': ['2008-07-03', '2008-07-11', '2008-07-14'], 
        'interval': ['daily', 'daily', 'daily'], 
        'unit': ['dollars per barrel', 'dollars per barrel', 'dollars per barrel'], 
        'symbol': ['DCOILBRENTEU', 'DCOILBRENTEU', 'DCOILBRENTEU'],
        'rnk': [1,2,3]
        }
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_not_date_1_and_rank_type_bottom_and_rank_position_3():
    # Define input parameters
    product = 'Brent'
    date_1 = None
    date_2 = None
    interval = None
    rank_type = "bottom"
    rank_position = 3

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = ("WITH ranked AS ("
                          "SELECT name, value AS price, date, interval, unit, symbol, DENSE_RANK() OVER(ORDER BY value ASC) as rnk "
                          "FROM commodities "
                          "WHERE name = %s ) "
                          "SELECT * FROM ranked WHERE rnk <= %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', 3)
    
    data = {'name': ['Brent', 'Brent', 'Brent'], 
        'price': [9.10, 9.12, 9.26], 
        'date': ['1998-12-10', '2020-04-21', '1998-12-11'], 
        'interval': ['daily', 'daily', 'daily'], 
        'unit': ['dollars per barrel', 'dollars per barrel', 'dollars per barrel'], 
        'symbol': ['DCOILBRENTEU', 'DCOILBRENTEU', 'DCOILBRENTEU'],
        'rnk': [1,2,3]
        }
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()
    
def test_create_sql_query_not_date_1_and_rank_type_max_and_rank_position_3():
    # Define input parameters
    product = 'Brent'
    date_1 = None
    date_2 = None
    interval = None
    rank_type = "max"
    rank_position = 3

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = ("WITH ranked AS ("
                          "SELECT name, value AS price, date, interval, unit, symbol, DENSE_RANK() OVER(ORDER BY value DESC) as rnk "
                          "FROM commodities "
                          "WHERE name = %s ) "
                          "SELECT * FROM ranked WHERE rnk = %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', 3)
    
    data = {'name': ['Brent'], 
        'price': [142.43], 
        'date': ['2008-07-14'], 
        'interval': ['daily'], 
        'unit': ['dollars per barrel'], 
        'symbol': ['DCOILBRENTEU'],
        'rnk': [3]
        }
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_not_date_1_and_rank_type_min_and_rank_position_3():
    # Define input parameters
    product = 'Brent'
    date_1 = None
    date_2 = None
    interval = None
    rank_type = "min"
    rank_position = 3

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = ("WITH ranked AS ("
                          "SELECT name, value AS price, date, interval, unit, symbol, DENSE_RANK() OVER(ORDER BY value ASC) as rnk "
                          "FROM commodities "
                          "WHERE name = %s ) "
                          "SELECT * FROM ranked WHERE rnk = %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', 3)
    
    data = {'name': ['Brent'], 
        'price': [9.26], 
        'date': ['1998-12-11'], 
        'interval': ['daily'], 
        'unit': ['dollars per barrel'], 
        'symbol': ['DCOILBRENTEU'],
        'rnk': [3]
        }
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

#if product and date_1 and not date_2:
#    if  not rank_type:

def test_create_sql_query_date_1_and_not_rank_type_case_annual_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010'
    date_2 = None
    interval = 'annual'
    rank_type = None
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, SUM(value) / COUNT(value) AS price, interval, unit "
        "FROM commodities "
        "WHERE name = %s AND to_char(date, 'YYYY') = %s "
        "GROUP BY name, interval, unit "
        "ORDER BY name;"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010')
    
    data = {
        'name': ['Brent'], 
        'price': [79.60944444444449], 
        'interval': ['daily'], 
        'unit': ['dollars per barrel']
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_and_not_rank_type_case_monthly_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11'
    date_2 = None
    interval = 'monthly'
    rank_type = None
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, SUM(value) / COUNT(value) AS price, interval, unit "
        "FROM commodities "
        "WHERE name = %s AND to_char(date, 'YYYY-MM') = %s "
        "GROUP BY name, interval, unit "
        "ORDER BY name;"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11')
    
    data = {
        'name': ['Brent'], 
        'price': [85.2747619047619], 
        'interval': ['daily'], # it's not the same interval as in the variable
        'unit': ['dollars per barrel']
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_and_not_rank_type_case_daily_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11-05'
    date_2 = None
    interval = 'daily'
    rank_type = None
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, SUM(value) / COUNT(value) AS price, interval, unit "
        "FROM commodities "
        "WHERE name = %s AND to_char(date, 'YYYY-MM-DD') = %s AND interval = %s "
        "GROUP BY name, interval, unit "
        "ORDER BY name;"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11-05', 'daily')
    
    data = {
        'name': ['Brent'], 
        'price': [87.05], 
        'interval': ['daily'], # it's not the same interval as in the variable
        'unit': ['dollars per barrel']
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

#if product and date_1 and not date_2:
#    if  not rank_type:
#        ...
#    elif rank_type and not rank_position:

def test_create_sql_query_date_1_and_rank_type_top_case_annual_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010'
    date_2 = None
    interval = 'annual'
    rank_type = 'top'
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, value AS price, date, interval, unit, DENSE_RANK() OVER(ORDER BY value DESC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY') = %s "
        ")"
        "SELECT * FROM ranked WHERE rnk = 1"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010')
    
    data = {
        'name': ['Brent'], 
        'price': [93.63], 
        'date': ['2010-12-23'], 
        'interval': ['daily'], 
        'unit': ['dollars per barrel'], 
        'rnk': [1]
        }
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_and_rank_type_max_case_annual_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010'
    date_2 = None
    interval = 'annual'
    rank_type = 'max'
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, value AS price, date, interval, unit, DENSE_RANK() OVER(ORDER BY value DESC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY') = %s "
        ")"
        "SELECT * FROM ranked WHERE rnk = 1"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010')
    
    data = {
        'name': ['Brent'], 
        'price': [93.63], 
        'date': ['2010-12-23'], 
        'interval': ['daily'], 
        'unit': ['dollars per barrel'], 
        'rnk': [1]
        }
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_and_rank_type_bottom_case_annual_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010'
    date_2 = None
    interval = 'annual'
    rank_type = 'bottom'
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, value AS price, date, interval, unit, DENSE_RANK() OVER(ORDER BY value ASC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY') = %s "
        ")"
        "SELECT * FROM ranked WHERE rnk = 1"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010')
    
    data = {
        'name': ['Brent'], 
        'price': [67.18], 
        'date': ['2010-05-25'], 
        'interval': ['daily'], 
        'unit': ['dollars per barrel'], 
        'rnk': [1]
        }
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()
    
def test_create_sql_query_date_1_and_rank_type_min_case_annual_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010'
    date_2 = None
    interval = 'annual'
    rank_type = 'min'
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, value AS price, date, interval, unit, DENSE_RANK() OVER(ORDER BY value ASC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY') = %s "
        ")"
        "SELECT * FROM ranked WHERE rnk = 1"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010')
    
    data = {
        'name': ['Brent'], 
        'price': [67.18], 
        'date': ['2010-05-25'], 
        'interval': ['daily'], 
        'unit': ['dollars per barrel'], 
        'rnk': [1]
        }
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_and_rank_type_top_case_monthly_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11'
    date_2 = None
    interval = 'monthly'
    rank_type = 'top'
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, value AS price, date, interval, unit, DENSE_RANK() OVER(ORDER BY value DESC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM') = %s "
        ")"
        "SELECT * FROM ranked WHERE rnk = 1"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11')
    
    data = {
        'name': ['Brent'], 
        'price': [88.08], 
        'date': ['2010-11-11'], 
        'interval': ['daily'], 
        'unit': ['dollars per barrel'], 
        'rnk': [1]
        }
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_and_rank_type_max_case_monthly_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11'
    date_2 = None
    interval = 'monthly'
    rank_type = 'max'
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, value AS price, date, interval, unit, DENSE_RANK() OVER(ORDER BY value DESC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM') = %s "
        ")"
        "SELECT * FROM ranked WHERE rnk = 1"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11')
    
    data = {
        'name': ['Brent'], 
        'price': [88.08], 
        'date': ['2010-11-11'], 
        'interval': ['daily'], 
        'unit': ['dollars per barrel'], 
        'rnk': [1]
        }
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_and_rank_type_bottom_case_monthly_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11'
    date_2 = None
    interval = 'monthly'
    rank_type = 'bottom'
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, value AS price, date, interval, unit, DENSE_RANK() OVER(ORDER BY value ASC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM') = %s "
        ")"
        "SELECT * FROM ranked WHERE rnk = 1"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11')
    
    data = {
        'name': ['Brent'], 
        'price': [82.34], 
        'date': ['2010-11-22'], 
        'interval': ['daily'], 
        'unit': ['dollars per barrel'], 
        'rnk': [1]
        }
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_and_rank_type_min_case_monthly_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11'
    date_2 = None
    interval = 'monthly'
    rank_type = 'min'
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, value AS price, date, interval, unit, DENSE_RANK() OVER(ORDER BY value ASC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM') = %s "
        ")"
        "SELECT * FROM ranked WHERE rnk = 1"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11')
    
    data = {
        'name': ['Brent'], 
        'price': [82.34], 
        'date': ['2010-11-22'], 
        'interval': ['daily'], 
        'unit': ['dollars per barrel'], 
        'rnk': [1]
        }
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

#if product and date_1 and not date_2:
#    if  not rank_type:
#        ...
#    elif rank_type and not rank_position:
#        ...
#    elif rank_type and rank_position:

def test_create_sql_query_date_1_and_rank_type_top_and_rank_position_3_case_annual_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010'
    date_2 = None
    interval = 'annual'
    rank_type = 'top'
    rank_position = 3

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, value AS price, date, interval, unit, DENSE_RANK() OVER(ORDER BY value DESC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY') = %s "
        ")"
        "SELECT * FROM ranked WHERE rnk <= %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010', 3)
    
    data = {
        'name': ['Brent', 'Brent', 'Brent', 'Brent'], 
        'price': [93.63, 93.55, 93.52, 93.52], 
        'date': ['2010-12-23', '2010-12-22', '2010-12-29', '2010-12-28'], 
        'interval': ['daily', 'daily', 'daily', 'daily'], 
        'unit': ['dollars per barrel', 'dollars per barrel', 'dollars per barrel', 'dollars per barrel'], 
        'rnk': [1, 2, 3, 3]
        }
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_and_rank_type_bottom_and_rank_position_3_case_annual_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010'
    date_2 = None
    interval = 'annual'
    rank_type = 'bottom'
    rank_position = 3

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, value AS price, date, interval, unit, DENSE_RANK() OVER(ORDER BY value ASC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY') = %s "
        ")"
        "SELECT * FROM ranked WHERE rnk <= %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010', 3)
    
    data = {
        'name': ['Brent', 'Brent', 'Brent', 'Brent'], 
        'price': [67.18, 69.56, 69.62, 69.62], 
        'date': ['2010-05-25', '2010-05-20', '2010-02-08', '2010-05-24'], 
        'interval': ['daily', 'daily', 'daily', 'daily'], 
        'unit': ['dollars per barrel', 'dollars per barrel', 'dollars per barrel', 'dollars per barrel'], 
        'rnk': [1, 2, 3, 3]
        }
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_and_rank_type_max_and_rank_position_3_case_annual_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010'
    date_2 = None
    interval = 'annual'
    rank_type = 'max'
    rank_position = 3

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, value AS price, date, interval, unit, DENSE_RANK() OVER(ORDER BY value DESC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY') = %s "
        ")"
        "SELECT * FROM ranked WHERE rnk = %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010', 3)
    
    data = {
        'name': ['Brent', 'Brent'], 
        'price': [93.52, 93.52], 
        'date': ['2010-12-29', '2010-12-28'], 
        'interval': ['daily', 'daily'], 
        'unit': ['dollars per barrel', 'dollars per barrel'], 
        'rnk': [3, 3]
        }
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_and_rank_type_min_and_rank_position_3_case_annual_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010'
    date_2 = None
    interval = 'annual'
    rank_type = 'min'
    rank_position = 3

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, value AS price, date, interval, unit, DENSE_RANK() OVER(ORDER BY value ASC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY') = %s "
        ")"
        "SELECT * FROM ranked WHERE rnk = %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010', 3)
    
    data = {
        'name': ['Brent', 'Brent'], 
        'price': [69.62, 69.62], 
        'date': ['2010-02-08', '2010-05-24'], 
        'interval': ['daily', 'daily'], 
        'unit': ['dollars per barrel', 'dollars per barrel'], 
        'rnk': [3, 3]
        }
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_and_rank_type_top_and_rank_position_3_case_monthly_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11'
    date_2 = None
    interval = 'monthly'
    rank_type = 'top'
    rank_position = 3

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, value AS price, date, interval, unit, DENSE_RANK() OVER(ORDER BY value DESC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM') = %s "
        ")"
        "SELECT * FROM ranked WHERE rnk <= %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11', 3)
    
    data = {
        'name': ['Brent', 'Brent', 'Brent'], 
        'price': [88.08, 87.93, 87.92], 
        'date': ['2010-11-11', '2010-11-09', '2010-11-10'], 
        'interval': ['daily', 'daily', 'daily'], 
        'unit': ['dollars per barrel', 'dollars per barrel', 'dollars per barrel'], 
        'rnk': [1, 2, 3]
        }
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_and_rank_type_bottom_and_rank_position_3_case_monthly_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11'
    date_2 = None
    interval = 'monthly'
    rank_type = 'bottom'
    rank_position = 3

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, value AS price, date, interval, unit, DENSE_RANK() OVER(ORDER BY value ASC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM') = %s "
        ")"
        "SELECT * FROM ranked WHERE rnk <= %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11', 3)
    
    data = {
        'name': ['Brent', 'Brent', 'Brent'], 
        'price': [82.34, 82.37, 83.17], 
        'date': ['2010-11-22', '2010-11-23', '2010-11-19'], 
        'interval': ['daily', 'daily', 'daily'], 
        'unit': ['dollars per barrel', 'dollars per barrel', 'dollars per barrel'], 
        'rnk': [1, 2, 3]
        }
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_and_rank_type_max_and_rank_position_3_case_monthly_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11'
    date_2 = None
    interval = 'monthly'
    rank_type = 'max'
    rank_position = 3

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, value AS price, date, interval, unit, DENSE_RANK() OVER(ORDER BY value DESC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM') = %s "
        ")"
        "SELECT * FROM ranked WHERE rnk = %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11', 3)
    
    data = {
        'name': ['Brent'], 
        'price': [87.92], 
        'date': ['2010-11-10'], 
        'interval': ['daily'], 
        'unit': ['dollars per barrel'], 
        'rnk': [3]
        }
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_and_rank_type_min_and_rank_position_3_case_monthly_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11'
    date_2 = None
    interval = 'monthly'
    rank_type = 'min'
    rank_position = 3

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, value AS price, date, interval, unit, DENSE_RANK() OVER(ORDER BY value ASC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM') = %s "
        ")"
        "SELECT * FROM ranked WHERE rnk = %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11', 3)
    
    data = {
        'name': ['Brent'], 
        'price': [83.17], 
        'date': ['2010-11-19'], 
        'interval': ['daily'], 
        'unit': ['dollars per barrel'], 
        'rnk': [3]
        }
    
    expected_df = pd.DataFrame(data)
    # Convert the date string to a datetime.date object
    expected_df['date'] = pd.to_datetime(expected_df['date']).dt.date

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()


#if product and date_1 and date_2:
#    if not rank_type:

def test_create_sql_query_date_1_date_2_not_rank_type_case_annual_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010'
    date_2 = '2011'
    interval = 'annual'
    rank_type = None
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY') as date, unit "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY'), unit ORDER BY TO_CHAR(date, 'YYYY');"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010', '2011')
    
    data = {
        'name': ['Brent', 'Brent'], 
        'price': [79.60944444444449, 111.26427419354837], 
        'date': ['2010', '2011'], 
        'unit': ['dollars per barrel', 'dollars per barrel'] 
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_not_rank_type_case_monthly_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11'
    date_2 = '2010-12'
    interval = 'monthly'
    rank_type = None
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY-MM') as date, unit "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY-MM'), unit ORDER BY TO_CHAR(date, 'YYYY-MM');"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11', '2010-12')
    
    data = {
        'name': ['Brent', 'Brent'], 
        'price': [85.2747619047619, 91.4468181818182], 
        'date': ['2010-11', '2010-12'], 
        'unit': ['dollars per barrel', 'dollars per barrel'] 
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_not_rank_type_case_daily_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11-01'
    date_2 = '2010-11-02'
    interval = 'daily'
    rank_type = None
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY-MM-DD') as date, unit "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM-DD') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY-MM-DD'), unit ORDER BY TO_CHAR(date, 'YYYY-MM-DD');"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11-01', '2010-11-02')
    
    data = {
        'name': ['Brent', 'Brent'], 
        'price': [84.06, 84.71], 
        'date': ['2010-11-01', '2010-11-02'], 
        'unit': ['dollars per barrel', 'dollars per barrel'] 
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

#if product and date_1 and date_2:
#    if not rank_type:
#        ...
#    elif rank_type and not rank_position:

def test_create_sql_query_date_1_date_2_rank_type_top_not_rank_position_case_annual_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010'
    date_2 = '2011'
    interval = 'annual'
    rank_type = 'top'
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY') as date, unit "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY'), unit ORDER BY price DESC LIMIT 1;"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010', '2011')
    
    data = {
        'name': ['Brent'], 
        'price': [111.26427419354837], 
        'date': ['2011'], 
        'unit': ['dollars per barrel'] 
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_max_not_rank_position_case_annual_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010'
    date_2 = '2011'
    interval = 'annual'
    rank_type = 'max'
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY') as date, unit "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY'), unit ORDER BY price DESC LIMIT 1;"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010', '2011')
    
    data = {
        'name': ['Brent'], 
        'price': [111.26427419354837], 
        'date': ['2011'], 
        'unit': ['dollars per barrel'] 
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_bottom_not_rank_position_case_annual_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010'
    date_2 = '2011'
    interval = 'annual'
    rank_type = 'bottom'
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY') as date, unit "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY'), unit ORDER BY price ASC LIMIT 1;"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010', '2011')
    
    data = {
        'name': ['Brent'], 
        'price': [79.60944444444449], 
        'date': ['2010'], 
        'unit': ['dollars per barrel'] 
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_min_not_rank_position_case_annual_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010'
    date_2 = '2011'
    interval = 'annual'
    rank_type = 'min'
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY') as date, unit "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY'), unit ORDER BY price ASC LIMIT 1;"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010', '2011')
    
    data = {
        'name': ['Brent'], 
        'price': [79.60944444444449], 
        'date': ['2010'], 
        'unit': ['dollars per barrel'] 
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_top_not_rank_position_case_monthly_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11'
    date_2 = '2010-12'
    interval = 'monthly'
    rank_type = 'top'
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY-MM') as date, unit "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY-MM'), unit ORDER BY price DESC LIMIT 1;"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11', '2010-12')
    
    data = {
        'name': ['Brent'], 
        'price': [91.4468181818182], 
        'date': ['2010-12'], 
        'unit': ['dollars per barrel'] 
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_max_not_rank_position_case_monthly_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11'
    date_2 = '2010-12'
    interval = 'monthly'
    rank_type = 'max'
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY-MM') as date, unit "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY-MM'), unit ORDER BY price DESC LIMIT 1;"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11', '2010-12')
    
    data = {
        'name': ['Brent'], 
        'price': [91.4468181818182], 
        'date': ['2010-12'], 
        'unit': ['dollars per barrel'] 
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_bottom_not_rank_position_case_monthly_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11'
    date_2 = '2010-12'
    interval = 'monthly'
    rank_type = 'bottom'
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY-MM') as date, unit "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY-MM'), unit ORDER BY price ASC LIMIT 1;"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11', '2010-12')
    
    data = {
        'name': ['Brent'], 
        'price': [85.2747619047619], 
        'date': ['2010-11'], 
        'unit': ['dollars per barrel'] 
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_min_not_rank_position_case_monthly_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11'
    date_2 = '2010-12'
    interval = 'monthly'
    rank_type = 'min'
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY-MM') as date, unit "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY-MM'), unit ORDER BY price ASC LIMIT 1;"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11', '2010-12')
    
    data = {
        'name': ['Brent'], 
        'price': [85.2747619047619], 
        'date': ['2010-11'], 
        'unit': ['dollars per barrel'] 
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_top_not_rank_position_case_daily_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11-01'
    date_2 = '2010-11-02'
    interval = 'daily'
    rank_type = 'top'
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY-MM-DD') as date, unit "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM-DD') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY-MM-DD'), unit ORDER BY price DESC LIMIT 1;"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11-01', '2010-11-02')
    
    data = {
        'name': ['Brent'], 
        'price': [84.71], 
        'date': ['2010-11-02'], 
        'unit': ['dollars per barrel'] 
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_max_not_rank_position_case_daily_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11-01'
    date_2 = '2010-11-02'
    interval = 'daily'
    rank_type = 'max'
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY-MM-DD') as date, unit "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM-DD') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY-MM-DD'), unit ORDER BY price DESC LIMIT 1;"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11-01', '2010-11-02')
    
    data = {
        'name': ['Brent'], 
        'price': [84.71], 
        'date': ['2010-11-02'], 
        'unit': ['dollars per barrel'] 
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_bottom_not_rank_position_case_daily_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11-01'
    date_2 = '2010-11-02'
    interval = 'daily'
    rank_type = 'bottom'
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY-MM-DD') as date, unit "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM-DD') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY-MM-DD'), unit ORDER BY price ASC LIMIT 1;"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11-01', '2010-11-02')
    
    data = {
        'name': ['Brent'], 
        'price': [84.06], 
        'date': ['2010-11-01'], 
        'unit': ['dollars per barrel'] 
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_min_not_rank_position_case_daily_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11-01'
    date_2 = '2010-11-02'
    interval = 'daily'
    rank_type = 'min'
    rank_position = None

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY-MM-DD') as date, unit "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM-DD') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY-MM-DD'), unit ORDER BY price ASC LIMIT 1;"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11-01', '2010-11-02')
    
    data = {
        'name': ['Brent'], 
        'price': [84.06], 
        'date': ['2010-11-01'], 
        'unit': ['dollars per barrel'] 
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

#if product and date_1 and date_2:
#    if not rank_type:
#        ...
#    elif rank_type and not rank_position:
#        ...
#    elif rank_type and rank_position:

def test_create_sql_query_date_1_date_2_rank_type_top_rank_position_2_case_annual_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010'
    date_2 = '2011'
    interval = 'annual'
    rank_type = 'top'
    rank_position = 2

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY') as date, unit, DENSE_RANK() OVER(ORDER BY SUM(value) / COUNT(value) DESC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY'), unit "
        ")"
        "SELECT * FROM ranked WHERE rnk <= %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010', '2011', 2)
    
    data = {
        'name': ['Brent', 'Brent'], 
        'price': [111.26427419354837, 79.60944444444449], 
        'date': ['2011', '2010'], 
        'unit': ['dollars per barrel', 'dollars per barrel'],
        'rnk': [1, 2]
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_bottom_rank_position_2_case_annual_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010'
    date_2 = '2011'
    interval = 'annual'
    rank_type = 'bottom'
    rank_position = 2

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY') as date, unit, DENSE_RANK() OVER(ORDER BY SUM(value) / COUNT(value) ASC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY'), unit "
        ")"
        "SELECT * FROM ranked WHERE rnk <= %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010', '2011', 2)
    
    data = {
        'name': ['Brent', 'Brent'], 
        'price': [79.60944444444449, 111.26427419354837], 
        'date': ['2010', '2011'], 
        'unit': ['dollars per barrel', 'dollars per barrel'],
        'rnk': [1, 2]
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_max_rank_position_2_case_annual_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010'
    date_2 = '2011'
    interval = 'annual'
    rank_type = 'max'
    rank_position = 2

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY') as date, unit, DENSE_RANK() OVER(ORDER BY SUM(value) / COUNT(value) DESC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY'), unit "
        ")"
        "SELECT * FROM ranked WHERE rnk = %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010', '2011', 2)
    
    data = {
        'name': ['Brent'], 
        'price': [79.60944444444449], 
        'date': ['2010'], 
        'unit': ['dollars per barrel'],
        'rnk': [2]
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_min_rank_position_2_case_annual_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010'
    date_2 = '2011'
    interval = 'annual'
    rank_type = 'min'
    rank_position = 2

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY') as date, unit, DENSE_RANK() OVER(ORDER BY SUM(value) / COUNT(value) ASC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY'), unit "
        ")"
        "SELECT * FROM ranked WHERE rnk = %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010', '2011', 2)
    
    data = {
        'name': ['Brent'], 
        'price': [111.26427419354837], 
        'date': ['2011'], 
        'unit': ['dollars per barrel'],
        'rnk': [2]
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_top_rank_position_2_case_monthly_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-08'
    date_2 = '2010-11'
    interval = 'monthly'
    rank_type = 'top'
    rank_position = 2

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY-MM') as date, unit, DENSE_RANK() OVER(ORDER BY SUM(value) / COUNT(value) DESC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY-MM'), unit "
        ")"
        "SELECT * FROM ranked WHERE rnk <= %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-08', '2010-11', 2)
    
    data = {
        'name': ['Brent', 'Brent'], 
        'price': [85.2747619047619, 82.66476190476189], 
        'date': ['2010-11', '2010-10'], 
        'unit': ['dollars per barrel', 'dollars per barrel'],
        'rnk': [1, 2]
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_bottom_rank_position_2_case_monthly_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-08'
    date_2 = '2010-11'
    interval = 'monthly'
    rank_type = 'bottom'
    rank_position = 2

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY-MM') as date, unit, DENSE_RANK() OVER(ORDER BY SUM(value) / COUNT(value) ASC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY-MM'), unit "
        ")"
        "SELECT * FROM ranked WHERE rnk <= %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-08', '2010-11', 2)
    
    data = {
        'name': ['Brent', 'Brent'], 
        'price': [77.03954545454543, 77.8404761904762], 
        'date': ['2010-08', '2010-09'], 
        'unit': ['dollars per barrel', 'dollars per barrel'],
        'rnk': [1, 2]
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_max_rank_position_2_case_monthly_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-08'
    date_2 = '2010-11'
    interval = 'monthly'
    rank_type = 'max'
    rank_position = 2

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY-MM') as date, unit, DENSE_RANK() OVER(ORDER BY SUM(value) / COUNT(value) DESC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY-MM'), unit "
        ")"
        "SELECT * FROM ranked WHERE rnk = %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-08', '2010-11', 2)
    
    data = {
        'name': ['Brent'], 
        'price': [82.66476190476189], 
        'date': ['2010-10'], 
        'unit': ['dollars per barrel'],
        'rnk': [2]
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_min_rank_position_2_case_monthly_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-08'
    date_2 = '2010-11'
    interval = 'monthly'
    rank_type = 'min'
    rank_position = 2

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY-MM') as date, unit, DENSE_RANK() OVER(ORDER BY SUM(value) / COUNT(value) ASC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY-MM'), unit "
        ")"
        "SELECT * FROM ranked WHERE rnk = %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-08', '2010-11', 2)
    
    data = {
        'name': ['Brent'], 
        'price': [77.8404761904762], 
        'date': ['2010-09'], 
        'unit': ['dollars per barrel'],
        'rnk': [2]
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_top_rank_position_2_case_daily_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11-01'
    date_2 = '2010-11-05'
    interval = 'daily'
    rank_type = 'top'
    rank_position = 2

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY-MM-DD') as date, unit, DENSE_RANK() OVER(ORDER BY SUM(value) / COUNT(value) DESC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM-DD') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY-MM-DD'), unit "
        ")"
        "SELECT * FROM ranked WHERE rnk <= %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11-01', '2010-11-05', 2)
    
    data = {
        'name': ['Brent', 'Brent'], 
        'price': [87.05, 86.83], 
        'date': ['2010-11-05', '2010-11-04'], 
        'unit': ['dollars per barrel', 'dollars per barrel'],
        'rnk': [1, 2]
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_bottom_rank_position_2_case_daily_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11-01'
    date_2 = '2010-11-05'
    interval = 'daily'
    rank_type = 'bottom'
    rank_position = 2

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY-MM-DD') as date, unit, DENSE_RANK() OVER(ORDER BY SUM(value) / COUNT(value) ASC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM-DD') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY-MM-DD'), unit "
        ")"
        "SELECT * FROM ranked WHERE rnk <= %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11-01', '2010-11-05', 2)
    
    data = {
        'name': ['Brent', 'Brent'], 
        'price': [84.06, 84.71], 
        'date': ['2010-11-01', '2010-11-02'], 
        'unit': ['dollars per barrel', 'dollars per barrel'],
        'rnk': [1, 2]
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_max_rank_position_2_case_daily_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11-01'
    date_2 = '2010-11-05'
    interval = 'daily'
    rank_type = 'max'
    rank_position = 2

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY-MM-DD') as date, unit, DENSE_RANK() OVER(ORDER BY SUM(value) / COUNT(value) DESC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM-DD') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY-MM-DD'), unit "
        ")"
        "SELECT * FROM ranked WHERE rnk = %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11-01', '2010-11-05', 2)
    
    data = {
        'name': ['Brent'], 
        'price': [86.83], 
        'date': ['2010-11-04'], 
        'unit': ['dollars per barrel'],
        'rnk': [2]
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()

def test_create_sql_query_date_1_date_2_rank_type_min_rank_position_2_case_daily_interval():
    # Define input parameters
    product = 'Brent'
    date_1 = '2010-11-01'
    date_2 = '2010-11-05'
    interval = 'daily'
    rank_type = 'min'
    rank_position = 2

    # Call the function to generate the SQL query
    sql_query, params = create_sql_query(product, date_1, date_2, interval, rank_type, rank_position)
    
    # Execute the SQL query against the test database
    df = pd.read_sql_query(sql_query, db_connect(), params=params)

    # Define the expected SQL query
    expected_sql_query = (
        "WITH ranked AS ("
        "SELECT name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY-MM-DD') as date, unit, DENSE_RANK() OVER(ORDER BY SUM(value) / COUNT(value) ASC) as rnk "
        "FROM commodities "
        "WHERE name = %s AND TO_CHAR(date, 'YYYY-MM-DD') BETWEEN %s AND %s "
        "GROUP BY name, TO_CHAR(date, 'YYYY-MM-DD'), unit "
        ")"
        "SELECT * FROM ranked WHERE rnk = %s"
    )

    # Define the expected parameters
    expected_params = ('Brent', '2010-11-01', '2010-11-05', 2)
    
    data = {
        'name': ['Brent'], 
        'price': [84.71], 
        'date': ['2010-11-02'], 
        'unit': ['dollars per barrel'],
        'rnk': [2]
        }
    
    expected_df = pd.DataFrame(data)

    # Assert that the generated SQL query and parameters match the expected ones
    assert sql_query == expected_sql_query
    assert params == expected_params
    assert expected_df.to_dict() == df.to_dict()