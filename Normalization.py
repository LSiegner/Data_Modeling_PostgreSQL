# -*- coding: utf-8 -*-
"""
Created on Mon Dec 26 00:50:22 2022

@author: siegn
"""

import psycopg2


try: 
    conn = psycopg2.connect( database="postgres", user='postgres', password='LeonSiegner', host='127.0.0.1', port= '5432')
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)
try: 
    cur = conn.cursor()
except psycopg2.Error as e: 
    print("Error: Could not get cursor to the Database")
    print(e)
conn.set_session(autocommit=True)

# TO-DO: Add the CREATE Table Statement and INSERT statements to add the data in the table
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS music_store (transaction_id int , customer_name varchar, \
    cashier_name varchar, year int, albums_purchased text[]);")

except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)
    
try: 
    cur.execute("INSERT INTO music_store (transaction_id, customer_name, cashier_name,year,albums_purchased) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (1,"Amanda","Sam",2000,['Rubber Soul','Let it Be']))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO music_store (transaction_id, customer_name, cashier_name,year,albums_purchased) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (2,"Toby","Sam",2000,['My Generation']))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO music_store (transaction_id, customer_name, cashier_name,year,albums_purchased) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (3,"Max","Bob",2018,['Meet the Beatles', 'Help']))
except psycopg2.Error as e:
    print("Error: Inserting Rows")
    print (e)
    
    
try: 
    cur.execute("SELECT * FROM music_store;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

print("starting table")
print("--------------\n\n") 
row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()
   
   
   
'''Moving to first normal form'''
'''---------------------------'''



## TO-DO: Complete the CREATE table statements and INSERT statements

try: 
    cur.execute("CREATE TABLE IF NOT EXISTS music_store2 (transaction_id int , customer_name varchar, \
    cashier_name varchar, year int, album_purchased varchar);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)
    
try: 
    cur.execute("INSERT INTO music_store2 (transaction_id,customer_name,cashier_name,year,album_purchased) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (1,"Amanda","Sam",2000,"Rubber Soul"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO music_store2 (transaction_id,customer_name,cashier_name,year,album_purchased) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (1,"Amanda","Sam",2000,"Let it Be"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO music_store2 (transaction_id,customer_name,cashier_name,year,album_purchased) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (2,"Toby", "Sam",2000,"My Generation"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO music_store2 (transaction_id,customer_name,cashier_name,year,album_purchased) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (3,"Max","Bob",2018,"Meet the Beatles"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO music_store2 (transaction_id,customer_name,cashier_name,year,album_purchased) \
                 VALUES (%s, %s, %s, %s, %s)", \
                 (3,"Max","Bob",2018,"Help"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("SELECT * FROM music_store2;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

print("\n\nafter first normalization")
print("--------------------------\n\n") 
row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()
   
   
   
   
''' Moving to second normal form'''
'''-----------------------------'''




try: 
    cur.execute("CREATE TABLE IF NOT EXISTS transactions (transaction_id int, customer_name varchar, \
                cashier_name varchar , year int);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

try: 
    cur.execute("CREATE TABLE IF NOT EXISTS albums_sold (album_id int , transaction_id int, album_name varchar);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)


try: 
    cur.execute("INSERT INTO transactions (transaction_id , customer_name, cashier_name, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (1,"Amanda","Sam",2000))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO transactions (transaction_id , customer_name, cashier_name, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (2,"Toby","Sam",2000))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO transactions (transaction_id , customer_name, cashier_name, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (3,"Max","Bob",2018))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO albums_sold (album_id,transaction_id, album_name)\
                 VALUES (%s, %s, %s)", \
                 (1, 1,"Rubber Soul"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO albums_sold(album_id,transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (2, 1, "Let it Be"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO albums_sold (album_id,transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (3,2,"My Generation"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO albums_sold (album_id,transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (4,3,"Meet the Beatles"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO albums_sold(album_id,transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (5, 3,"Help"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

print("\n\nafter second normalization") 
print("------------------------- \n\n") 

print("Table: transactions\n")
try: 
    cur.execute("SELECT * FROM transactions;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

print("\nTable: albums_sold\n")
try: 
    cur.execute("SELECT * FROM albums_sold;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)
      
row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()
   
   
'''Moving to 3rd normal form'''
'''-------------------------'''

try: 
    cur.execute("CREATE TABLE IF NOT EXISTS transactions2 (transaction_id int, customer_name varchar , \
                cashier_id int, year int);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

try: 
    cur.execute("CREATE TABLE IF NOT EXISTS employees (cashier_id int, employee_name varchar);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

try: 
    cur.execute("INSERT INTO transactions2 (transaction_id,customer_name,cashier_id,year) \
                 VALUES (%s, %s, %s, %s)", \
                 (1,"Amanda",1,2000))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO transactions2  (transaction_id,customer_name,cashier_id,year) \
                 VALUES (%s, %s, %s, %s)", \
                 (2,"Toby",1,2000))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO transactions2  (transaction_id,customer_name,cashier_id,year) \
                 VALUES (%s, %s, %s, %s)", \
                 (3,"Max",2,2000))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO employees (cashier_id, employee_name) \
                 VALUES (%s, %s)", \
                 (1,"Sam"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO employees (cashier_id, employee_name) \
                 VALUES (%s, %s)", \
                 (2,"Bob"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)    

print("\n\nafter third normalization")
print("-------------------------")

print("Table: transactions2\n")
try: 
    cur.execute("SELECT * FROM transactions2;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

print("\nTable: albums_sold\n")
try: 
    cur.execute("SELECT * FROM albums_sold;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

print("\nTable: employees\n")
try: 
    cur.execute("SELECT * FROM employees;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()
   
try: 
    cur.execute("DROP table music_store")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table music_store2")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table transactions ")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table albums_sold")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table transactions2")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table employees")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
    
   
   