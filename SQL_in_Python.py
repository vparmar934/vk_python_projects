# -*- coding: utf-8 -*-
"""
Created on Sun Feb  5 11:46:53 2023

@author: vivek
"""

import psycopg2 as psy
import pandas as pd

try:
    con = psy.connect("host=localhost dbname=postgres user=postgres password=Pass@9588")
except psy.Error as e:
    print(e)
    
try:
    cur = con.cursor()
except psy.Error as e:
    print(e)

con.set_session(autocommit=True)

try:
    cur.execute("CREATE DATABASE db_practice1;")
except psy.Error as e:
    print(e)


cur.close()
con.close()

#------------------------------------------------------------------------------------------------------------------#

try:
    con = psy.connect("host=localhost dbname=db_practice1 user=postgres password=Pass@9588")
except psy.Error as e:
    print(e)

try:
    cur = con.cursor()
except psy.Error as e:
    print(e)

con.set_session(autocommit=True)

try:
    cur.execute("CREATE TABLE carsales(manufacturer VARCHAR, model VARCHAR, sales FLOAT, type VARCHAR, launch_date DATE);")
except psy.Error as e:
    print(e)

#Load & Clean dataset
cs = pd.read_csv(r"C:\Users\vivek\Downloads\Datasets\Car_sales.csv")
print(cs.head())

cs_clean = cs[['Manufacturer','Model','Sales_in_thousands','Vehicle_type','Latest_Launch']]
print(cs_clean)
print(cs_clean.dtypes)

cs_clean['Latest_Launch'] = pd.to_datetime(cs_clean['Latest_Launch'])
print(cs_clean.dtypes)
print(cs_clean)

#Insert data into database table
for i, row in cs_clean.iterrows():
    cur.execute("INSERT INTO carsales(manufacturer,model,sales,type,launch_date)\
                VALUES(%s,%s,%s,%s,%s)", list(row))

cur.execute("DELETE FROM carsales;")
                    
cur.execute("SELECT * FROM carsales LIMIT 5;")
    
row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()

#------------------------------------------------------------------------------------------------------------------#

supermarket_data = pd.read_csv(r"C:\Users\vivek\Downloads\Datasets\supermarket_sales.csv")
supermarket_data['Date'] = pd.to_datetime(supermarket_data['Date'])
supermarket_data_clean = supermarket_data[['Invoice ID','City','Gender','Product line','Total','Date','Payment','gross income','Rating']]
print(supermarket_data_clean.dtypes)

cur.execute('CREATE TABLE supermarket(invoice_ID VARCHAR PRIMARY KEY, city VARCHAR, gender VARCHAR, Product_line VARCHAR, total FLOAT,\
            date DATE, payment VARCHAR, gross_income FLOAT, rating FLOAT);')

#cur.execute('DROP TABLE supermarket;')

cur.execute('SELECT * FROM supermarket LIMIT 5;')

row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()

for i, row in supermarket_data_clean.iterrows():    
    cur.execute('INSERT INTO supermarket(invoice_ID, city, gender, Product_line, total, date, payment, gross_income, rating)\
             VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);', list(row))

cur.execute('SELECT gender,payment FROM supermarket LIMIT 5;')

row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()

cur.execute('SELECT city,gender,total,date,gross_income FROM supermarket ORDER BY city ASC LIMIT 5;')
row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()
    
cur.execute('SELECT city,gender, ROUND(SUM(total)) AS total_sales, ROUND(AVG(total)) AS avg_sales\
            FROM supermarket WHERE city = %s GROUP BY city,gender ORDER BY total_sales DESC LIMIT 10;',('Yangon',))
row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()

cur.execute('SELECT * FROM supermarket LIMit 5;')
row = cur.fetchone()
while row:
    print(row)
    row = cur.fetchone()