# -*- coding: utf-8 -*-
"""
Created on Wed Jan 31 18:53:39 2024

@author: John
"""
import psycopg2 # to connect to a DB
from bot_utils import bot_operations

# Explicitly tell config where the settings file is.
config = bot_operations.get_config()

# Create a database connection
def db_connect():
    
    # Fetch database credentials
    db_password = config('DB_PASSWORD', default='')
    db_login = config('DB_LOGIN', default='')
    db_name = config('DB_NAME', default='')
    db_host = config('DB_HOST', default='')
            
    return psycopg2.connect(dbname=db_name, user=db_login, password=db_password, host=db_host)


def create_sql_query(product, date_1, date_2, interval):
    
    if product and not date_1:
        fields = "name, value AS price, date, interval, unit, symbol"
        table = "commodities"
        conditions = "name = %s"
        order = "date DESC LIMIT 1"
        sql = (f"SELECT {fields} "
               f"FROM {table} "
               f"WHERE {conditions} "
               f"ORDER BY {order};")
        params = (product,)
    
    if product and date_1 and not date_2:
        fields = "name, SUM(value) / COUNT(value) AS price, interval, unit"
        table = "commodities"
        group_by = "name, interval, unit"
        order = "name"        
        conditions = (
            "name = %s AND to_char(date, 'YYYY') = %s" if interval == 'annually' 
            else "name = %s AND to_char(date, 'YYYY-MM') = %s" if interval == 'monthly'
            else "name = %s AND to_char(date, 'YYYY-MM-DD') = %s AND interval = %s" if interval == 'daily'
            else "failed interval"
        )
        sql = (f"SELECT {fields} "
               f"FROM {table} "
               f"WHERE {conditions} "
               f"GROUP BY {group_by} "
               f"ORDER BY {order};")
        params = (
            (product, date_1) if interval == 'annually'
            else (product, date_1) if interval == 'monthly'
            else (product, date_1, interval) if interval == 'daily'
            else None
        )       
    
    if product and date_1 and date_2:
        if interval == 'annually':
            fields = "name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY') as date, unit"
            table = "commodities"
            group_by = "name, TO_CHAR(date, 'YYYY'), unit"
            order = "TO_CHAR(date, 'YYYY')"
            conditions = "name = %s AND TO_CHAR(date, 'YYYY') BETWEEN %s AND %s"
            sql = (f"SELECT {fields} "
                   f"FROM {table} "
                   f"WHERE {conditions} "
                   f"GROUP BY {group_by} "
                   f"ORDER BY {order};")
        elif interval == 'monthly':        
            fields = "name, SUM(value) / COUNT(value) as price, TO_CHAR(date, 'YYYY-MM') as date, unit"
            table = "commodities"
            group_by = "name, TO_CHAR(date, 'YYYY-MM'), unit"
            order = "TO_CHAR(date, 'YYYY-MM')"
            conditions = "name = %s AND TO_CHAR(date, 'YYYY-MM') BETWEEN %s AND %s"
            sql = (f"SELECT {fields} "
                   f"FROM {table} "
                   f"WHERE {conditions} "
                   f"GROUP BY {group_by} "
                   f"ORDER BY {order};")
        elif interval == 'daily': 
            fields = "name, SUM(value) / COUNT(value) as price, date, unit"
            table = "commodities"
            group_by = "name, date, unit"
            order = "date"
            conditions = "name = %s AND date BETWEEN %s AND %s"
            sql = (f"SELECT {fields} "
                   f"FROM {table} "
                   f"WHERE {conditions} "
                   f"GROUP BY {group_by} "
                   f"ORDER BY {order};") 
        
        params = (product, date_1, date_2)
        
    return sql, params