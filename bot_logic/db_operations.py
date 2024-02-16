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


def create_sql_query(product, date_1, date_2, interval, rank_type, rank_position):
    
    if product and not date_1:
        if not rank_type:
            fields = "name, value AS price, date, interval, unit, symbol"
            table = "commodities"
            conditions = "name = %s"
            order = "date DESC LIMIT 1"
            sql = (f"SELECT {fields} "
                   f"FROM {table} "
                   f"WHERE {conditions} "
                   f"ORDER BY {order};")
            params = (product,)
        elif rank_type and not rank_position:
            order_direction = 'DESC' if rank_type in ['top','max'] else 'ASC' if rank_type in ['bottom', 'min'] else None
            #
            fields = "name, value AS price, date, interval, unit, symbol"
            table = "commodities"
            conditions = "name = %s"
            order = f"price {order_direction} LIMIT 1"
            sql = (f"SELECT {fields} "
                   f"FROM {table} "
                   f"WHERE {conditions} "
                   f"ORDER BY {order};")
            params = (product,)
        elif rank_type and rank_position:
            
            # determine order and comparison sign based on rank_type
            order_direction = 'DESC' if rank_type in ['top','max'] else 'ASC' if rank_type in ['bottom', 'min'] else None
            comparison = '<=' if rank_type in ['top', 'bottom'] else '=' if rank_type in ['max', 'min'] else None
        
            # define common parts of the SQL query
            fields = f"name, value AS price, date, interval, unit, symbol, DENSE_RANK() OVER(ORDER BY value {order_direction}) as rnk"
            table = "commodities"
            conditions = "name = %s"
        
            # construct the final SQL query
            sql = (
                f"WITH ranked AS ("
                f"SELECT {fields} "
                f"FROM {table} "
                f"WHERE {conditions} "
                f") "
                f"SELECT * FROM ranked WHERE rnk {comparison} %s"
            )
            params = (product, rank_position)
    
    if product and date_1 and not date_2:
        if  not rank_type:
            # Define common parts of the SQL query
            fields = "name, SUM(value) / COUNT(value) AS price, interval, unit"
            table = "commodities"
            group_by = "name, interval, unit"
            order = "name"        
        
            # Determine conditions based on interval
            if interval == 'annually':
                conditions = "name = %s AND to_char(date, 'YYYY') = %s"
                params = (product, date_1)
            elif interval == 'monthly':
                conditions = "name = %s AND to_char(date, 'YYYY-MM') = %s"
                params = (product, date_1)
            elif interval == 'daily':
                conditions = "name = %s AND to_char(date, 'YYYY-MM-DD') = %s AND interval = %s"
                params = (product, date_1, interval)
            else:
                pass
            sql = (
                f"SELECT {fields} "
                f"FROM {table} "
                f"WHERE {conditions} "
                f"GROUP BY {group_by} "
                f"ORDER BY {order};"
                )
        elif rank_type and not rank_position:
            date_format = 'YYYY' if interval == 'annually' else 'YYYY-MM' if interval == 'monthly' else 'YYYY-MM-DD'
            order_direction = 'DESC' if rank_type in ['top','max'] else 'ASC' if rank_type in ['bottom', 'min'] else None
            #
            fields = f"name, value AS price, date, interval, unit, DENSE_RANK() OVER(ORDER BY value {order_direction}) as rnk"
            table = "commodities"
            conditions = f"name = %s AND TO_CHAR(date, '{date_format}') = %s"
            order = ""
            sql = (f"WITH ranked AS "
                   f"("
                   f"SELECT {fields} "
                   f"FROM {table} "
                   f"WHERE {conditions} "
                   f")"
                   f"SELECT * FROM ranked WHERE rnk = 1")   
            params = (product, date_1)             
        elif rank_type and rank_position:
            
            date_format = 'YYYY' if interval == 'annually' else 'YYYY-MM' if interval == 'monthly' else 'YYYY-MM-DD'
            order_direction = 'DESC' if rank_type in ['top','max'] else 'ASC' if rank_type in ['bottom', 'min'] else None
            comparison = '<=' if rank_type in ['top', 'bottom'] else '=' if rank_type in ['max', 'min'] else None
            #
            fields = f"name, value AS price, date, interval, unit, DENSE_RANK() OVER(ORDER BY value {order_direction}) as rnk"
            table = "commodities"
            conditions = f"name = %s AND TO_CHAR(date, '{date_format}') = %s"
            order = ""
            sql = (f"WITH ranked AS "
                   f"("
                   f"SELECT {fields} "
                   f"FROM {table} "
                   f"WHERE {conditions} "
                   f")"
                   f"SELECT * FROM ranked WHERE rnk {comparison} %s")   
            params = (product, date_1, rank_position)       
    
    if product and date_1 and date_2:
        if not rank_type:
            date_format = 'YYYY' if interval == 'annually' else 'YYYY-MM' if interval == 'monthly' else 'YYYY-MM-DD'
            #
            fields = f"name, SUM(value) / COUNT(value) as price, TO_CHAR(date, '{date_format}') as date, unit"
            table = "commodities"
            group_by = f"name, TO_CHAR(date, '{date_format}'), unit"
            order = f"TO_CHAR(date, '{date_format}')"
            conditions = f"name = %s AND TO_CHAR(date, '{date_format}') BETWEEN %s AND %s"
            sql = (f"SELECT {fields} "
                   f"FROM {table} "
                   f"WHERE {conditions} "
                   f"GROUP BY {group_by} "
                   f"ORDER BY {order};")
            params = (product, date_1, date_2)
        elif rank_type and not rank_position:
            #
            date_format = 'YYYY' if interval == 'annually' else 'YYYY-MM' if interval == 'monthly' else 'YYYY-MM-DD'
            order_direction = 'DESC' if rank_type in ['top','max'] else 'ASC' if rank_type in ['bottom', 'min'] else None
            #    
            fields = f"name, SUM(value) / COUNT(value) as price, TO_CHAR(date, '{date_format}') as date, unit"
            table = "commodities"
            group_by = f"name, TO_CHAR(date, '{date_format}'), unit"
            order = f"price {order_direction} LIMIT 1"
            conditions = "name = %s AND TO_CHAR(date, 'YYYY') BETWEEN %s AND %s"
            sql = (f"SELECT {fields} "
                   f"FROM {table} "
                   f"WHERE {conditions} "
                   f"GROUP BY {group_by} "
                   f"ORDER BY {order};")
            params = (product, date_1, date_2)
        elif rank_type and rank_position:
            #
            date_format = 'YYYY' if interval == 'annually' else 'YYYY-MM' if interval == 'monthly' else 'YYYY-MM-DD'
            order_direction = 'DESC' if rank_type in ['top','max'] else 'ASC' if rank_type in ['bottom', 'min'] else None
            comparison = '<=' if rank_type in ['top', 'bottom'] else '=' if rank_type in ['max', 'min'] else None
            #
            fields = f"name, SUM(value) / COUNT(value) as price, TO_CHAR(date, '{date_format}') as date, unit, DENSE_RANK() OVER(ORDER BY SUM(value) / COUNT(value) {order_direction}) as rnk"
            table = "commodities"
            group_by = f"name, TO_CHAR(date, '{date_format}'), unit"
            conditions = f"name = %s AND TO_CHAR(date, '{date_format}') BETWEEN %s AND %s"
            order = ""
            sql = (f"WITH ranked AS "
                   f"("
                   f"SELECT {fields} "
                   f"FROM {table} "
                   f"WHERE {conditions} "
                   f"GROUP BY {group_by} "
                   f")"
                   f"SELECT * FROM ranked WHERE rnk {comparison} %s")   
            params = (product, date_1, date_2, rank_position)   
        
    return sql, params