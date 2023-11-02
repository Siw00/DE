#EX.NO:1-Setup a Simple Data Engineering Development Infrastructure in My SQL Open source.
# Installing required libraries
pip install MySQL-client
pip install MySQL-connector-python

# Importing libraries
import sys
import mysql.connector
from mysql.connector import Error
import pandas as pd

connection = mysql.connector.connect(
    host='localhost',
    database='deelab',
    user='root',
    password='/0otU5E/',
    port=3306
)
cursor = connection.cursor()
print("Database connected successfully")

# Showing the sample data from the Database
command = cursor.execute("SELECT * FROM student_mark")
dataframe = pd.DataFrame(cursor.fetchall(), columns=['roll no', 'name', 'dept', 'mark'])
dataframe.head()

# Create a new table
command = cursor.execute("CREATE TABLE MY_TABLE (NAME VARCHAR(255), ADDRESS VARCHAR(255), PHONE_NO INT, EMAIL VARCHAR(255))")

# Insert values to the table
command = cursor.execute("INSERT INTO MY_TABLE VALUES ('ABIN', 'ARUMANAI', 123422537, 'asjadh@gmail.com')")

# Showing the new table
command = cursor.execute("SELECT * FROM MY_TABLE")
DF = pd.DataFrame(cursor.fetchall(), columns=['Name', 'Address', 'PhoneNo', 'Email'])
DF.head()

# Delete all data from the new table
command = cursor.execute("TRUNCATE TABLE MY_TABLE")
df1 = pd.DataFrame(cursor.fetchall(), columns=['Name', 'Address', 'PhoneNo', 'Email'])
df1.head()

# Deleting the new table
command = cursor.execute("DROP TABLE MY_TABLE")
df2 = pd.DataFrame(cursor.fetchall(), columns=['Name', 'Address', 'PhoneNo', 'Email'])
df2.head()



#EX.NO:2-Create the following tables with appropriate Columns.
# A Customer Table, A Product Table, A Sales Order Table
# Importing required libraries
import mysql.connector
from mysql.connector import Error
import pandas as pd

connection = mysql.connector.connect(
    host='localhost',
    database='deelab',
    user='root',
    password='/0otU5E/',
    port=3306
)
cursor = connection.cursor()
print("Database connected successfully")

# Creating CUSTOMER_DATA table
command = cursor.execute("CREATE TABLE CUSTOMER_DATA (CUSTOMER_ID INT, NAME VARCHAR(255), ADDRESS VARCHAR(255), LOCALITY VARCHAR(255), CITY VARCHAR(255), STATE VARCHAR(255), COUNTRY VARCHAR(255), POSTAL_CODE INT(6), EMAIL VARCHAR(255), Phone_Number BIGINT(20), PRIMARY KEY (CUSTOMER_ID))")

# Showing CUSTOMER_DATA table
command = cursor.execute("SELECT * FROM CUSTOMER_DATA")
df1 = pd.DataFrame(cursor.fetchall(), columns=['Customer_ID', 'Customer_Name', 'Customer_Address', 'Locality', 'City', 'State', 'Country', 'Postal_Code', 'Email_Address', 'Phone_Number'])
df1.head()

# Creating PRODUCT_INFO table
command = cursor.execute("CREATE TABLE PRODUCT(Product_ID INT, Customer_ID INT, Product_Family VARCHAR(80), Product_Group VARCHAR(255), Product VARCHAR(255), SKU VARCHAR(255), Unit_Price DECIMAL(10, 2), PRIMARY KEY (Product_ID))")

# Showing PRODUCT_INFO table
command = cursor.execute("SELECT * FROM PRODUCT")
df2 = pd.DataFrame(cursor.fetchall(), columns=['Product_ID', 'Customer_ID', 'Product_Family', 'Product_Group', 'Product', 'SKU', 'Unit_Price'])

# Creating the SALES_ORDER_DATA table
command = cursor.execute("CREATE TABLE Sales(Sales_Order_ID INT, Customer_ID INT, Product_ID INT, Sales_Order_Date DATE, Quantity VARCHAR(255), PRIMARY KEY (Sales_Order_ID)")
command = cursor.execute("SELECT * FROM Sales")
df3 = pd.DataFrame(cursor.fetchall(), columns=['Sales_Order_ID', 'Customer_ID', 'Product_ID', 'Sales_Order_Date', 'Quantity'])
df3.head()



#EX.NO:4-Validate data in the below data-loaded tables
#Customer Table, Product Table, and Sales Order Tables
# Import required libraries
import mysql.connector
import pandas as pd
from mysql.connector import Error

# Establish a database connection
connection = mysql.connector.connect(
    host='localhost',
    database='deelab',
    user='root',
    password='/0OTU5E/',
    port=3306
)
cursor = connection.cursor(buffered=True)
print("Database connected successfully")

# CUSTOMER TABLE
query = cursor.execute("SELECT COUNT(*) FROM CUSTOMER_DATA")
cursor.fetchall()

# Check if CUSTOMER_ID is unique
query = cursor.execute("SELECT COUNT(DISTINCT CUSTOMER_ID) FROM CUSTOMER_DATA")
cursor.fetchall()

# Check the data types of CUSTOMER NAMES and PRODUCT NAMES
query = cursor.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'DEELAB' AND TABLE_NAME = 'CUSTOMER_DATA'")
cursor.fetchall()

# PRODUCT TABLE
query = cursor.execute("SELECT COUNT(*) FROM PRODUCT")
cursor.fetchall()

query = cursor.execute("SELECT COUNT(DISTINCT PRODUCT_ID) FROM PRODUCT")
cursor.fetchall()

query = cursor.execute("SELECT COUNT(PRODUCT_ID) FROM PRODUCT WHERE CUSTOMER_ID = 0")
cursor.fetchall()

query = cursor.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'DEELAB' AND TABLE_NAME = 'PRODUCT'")
cursor.fetchall()

# SALES ORDER TABLE
query = cursor.execute("SELECT COUNT(*) FROM SALES")
cursor.fetchall()

# Check if SALES_ORDER_ID is unique
query = cursor.execute("SELECT COUNT(DISTINCT SALES_ORDER_ID) FROM SALES")
cursor.fetchall()

# Check if SALES_ORDER_ID is not null
query = cursor.execute("SELECT COUNT(SALES_ORDER_ID) FROM SALES WHERE SALES_ORDER_ID = 0")
cursor.fetchall()

query = cursor.execute("SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'DEELAB' AND TABLE_NAME = 'SALES'")
cursor.fetchall()



# EX no 5 Validate data in the below data-loaded tables. Check if the Customer Email has a format: name@email.com.Check if there are any null values and Check if there are any duplicate values.
# Import required libraries
import mysql.connector
import pandas as pd
from mysql.connector import Error

# Establish a database connection
connection = mysql.connector.connect(
    host='localhost',
    database='deelab',
    user='root',
    password='/0OTU5E/',
    port=3306
)
cursor = connection.cursor(buffered=True)
print("Database connected successfully")

# CUSTOMER TABLE
## Check if the customer email has a format: NAME@EMAIL.COM
query = cursor.execute("SELECT EMAIL FROM CUSTOMER_DATA WHERE EMAIL NOT LIKE '%_@_%._%'")
cursor.fetchall()

## Check if the PRODUCT_ID is not null
query = cursor.execute("SELECT * FROM CUSTOMER_DATA WHERE CUSTOMER_ID IS NULL")
query = cursor.execute("SELECT * FROM CUSTOMER_DATA WHERE NAME IS NULL")
query = cursor.execute("SELECT * FROM CUSTOMER_DATA WHERE ADDRESS IS NULL")
query = cursor.execute("SELECT * FROM CUSTOMER_DATA WHERE LOCALITY IS NULL")
query = cursor.execute("SELECT * FROM CUSTOMER_DATA WHERE CITY IS NULL")
query = cursor.execute("SELECT * FROM CUSTOMER_DATA WHERE STATE IS NULL")
query = cursor.execute("SELECT * FROM CUSTOMER_DATA WHERE COUNTRY IS NULL")
query = cursor.execute("SELECT * FROM CUSTOMER_DATA WHERE POSTAL_CODE IS NULL")
query = cursor.execute("SELECT * FROM CUSTOMER_DATA WHERE EMAIL IS NULL")
query = cursor.execute("SELECT * FROM CUSTOMER_DATA WHERE PHONE_NUMBER IS NULL")
cursor.fetchall()

## Check if there are any duplicate values
query = cursor.execute("SELECT CUSTOMER_ID, NAME, EMAIL FROM CUSTOMER_DATA ORDER BY EMAIL")
df = pd.DataFrame(cursor.fetchall(), columns=['CUSTOMER_ID', 'NAME', 'EMAIL'])
df.head()

# PRODUCT TABLE
## Check if there are any null values
query = cursor.execute("SELECT * FROM PRODUCT WHERE PRODUCT_ID IS NULL")
query = cursor.execute("SELECT * FROM PRODUCT WHERE CUSTOMER_ID IS NULL")
query = cursor.execute("SELECT * FROM PRODUCT WHERE PRODUCT_FAMILY IS NULL")
query = cursor.execute("SELECT * FROM PRODUCT WHERE PRODUCT_GROUP IS NULL")
query = cursor.execute("SELECT * FROM PRODUCT WHERE SKU IS NULL")
query = cursor.execute("SELECT * FROM PRODUCT WHERE UNIT_PRICE IS NULL")
cursor.fetchall()

## Check if there are any duplicate values
query = cursor.execute("SELECT PRODUCT_ID, CUSTOMER_ID, PRODUCT_FAMILY FROM PRODUCT ORDER BY PRODUCT_ID")
df1 = pd.DataFrame(cursor.fetchall(), columns=['PRODUCT_ID', 'CUSTOMER_ID', 'PRODUCT_FAMILY'])
df1.head()

# SALES TABLE
## Check if there are any null values
query = cursor.execute("SELECT * FROM SALES WHERE SALES_ORDER_ID IS NULL")
query = cursor.execute("SELECT * FROM SALES WHERE CUSTOMER_ID IS NULL")
query = cursor.execute("SELECT * FROM SALES WHERE PRODUCT_ID IS NULL")
query = cursor.execute("SELECT * FROM SALES WHERE SALES_ORDER_DATE IS NULL")
query = cursor.execute("SELECT * FROM SALES WHERE QUANTITY IS NULL")
cursor.fetchall()

## Check if there are any duplicate values
query = cursor.execute("SELECT SALES_ORDER_ID, CUSTOMER_ID, PRODUCT_ID FROM SALES ORDER BY SALES_ORDER_ID")
df2 = pd.DataFrame(cursor.fetchall(), columns=['SALES_ORDER_ID', 'CUSTOMER_ID', 'PRODUCT_ID'])
df2.head()



#EX.NO:8-Perform Update operation with the below logic using python
#Update Unit Price = $40 where Product = ‘Coolers’
#Update Quantity = 5 Customer = ‘Alfred’
import sys
import mysql.connector
from mysql.connector import Error
import pandas as pd

# Establish a database connection
connection = mysql.connector.connect(
    host='localhost',
    database='deelab',
    user='root',
    password='/0OTU5E/',
    port=3306
)
cursor = connection.cursor(buffered=True)
print("Database connected successfully")

# Retrieve data from FULL_DATA_MODEL table
query = cursor.execute("SELECT CUSTOMER_ID, CUSTOMER_NAME, SKU, UNIT_PRICE$, QUANTITY FROM FULL_DATA_MODEL")
df1 = pd.DataFrame(cursor.fetchall(), columns=['CUSTOMER_ID', 'CUSTOMER_NAME', 'SKU', 'UNIT_PRICE$', 'QUANTITY'])
df1.head(10)

# Update UNIT PRICE = 150 WHERE SKU = 'PUMA JACKET'
query = cursor.execute("UPDATE FULL_DATA_MODEL SET UNIT_PRICE$ = 150 WHERE SKU = 'PUMA JACKET'")

# Retrieve updated data
query = cursor.execute("SELECT CUSTOMER_ID, CUSTOMER_NAME, SKU, UNIT_PRICE$ FROM FULL_DATA_MODEL")
df = pd.DataFrame(cursor.fetchall(), columns=['CUSTOMER_ID', 'CUSTOMER_NAME', 'SKU', 'UNIT_PRICE$'])
df.head(10)

# Update QUANTITY = 6 WHERE CUSTOMER_ID = 21
query = cursor.execute("UPDATE FULL_DATA_MODEL SET QUANTITY = 6 WHERE CUSTOMER_ID = 21")

# Retrieve updated data
query = cursor.execute("SELECT CUSTOMER_ID, CUSTOMER_NAME, SKU, QUANTITY, UNIT_PRICE$ FROM FULL_DATA_MODEL")
df3 = pd.DataFrame(cursor.fetchall(), columns=['CUSTOMER_ID', 'CUSTOMER_NAME', 'SKU', 'UNIT_PRICE$', 'QUANTITY'])
df3.head(5)



#EX.NO:9-Perform the Update operation with the below logic using python
#Calculate Revenue as = Unit Price * Quantity
#Calculate 10% discount where Product = ‘sports jacket’
import sys
import mysql.connector
from mysql.connector import Error
import pandas as pd

# Establish a database connection
connection = mysql.connector.connect(
    host='localhost',
    database='deelab',
    user='root',
    password='/0OTU5E/',
    port=3306
)
cursor = connection.cursor(buffered=True)
print("Database connected successfully")

# Add a column to the FULL_DATA_MODEL table
query = cursor.execute("ALTER TABLE FULL_DATA_MODEL ADD DISCOUNTED_PRICE DECIMAL(8, 2)")

# Update DISCOUNTED_PRICE based on a condition
query = cursor.execute("UPDATE FULL_DATA_MODEL SET DISCOUNTED_PRICE = REVENUE * 0.5 WHERE QUANTITY = 3")

# Retrieve data with the new column
query = cursor.execute("SELECT CUSTOMER_ID, CUSTOMER_NAME, SKU, QUANTITY, UNIT_PRICE$, REVENUE, DISCOUNTED_PRICE FROM FULL_DATA_MODEL")
df = pd.DataFrame(cursor.fetchall(), columns=['CUSTOMER_ID', 'CUSTOMER_NAME', 'SKU', 'QUANTITY', 'UNIT_PRICE$', 'REVENUE', 'DISCOUNTED_PRICE'])
df.head(10)

# Commit the changes to the database
query = cursor.execute("COMMIT")



#EX.NO: 15-Remove duplicate rows and replace null values with #### and then update into table using Python in local host and use appropriate python libraries
import sys
import mysql.connector
from mysql.connector import Error
import pandas as pd
from sqlalchemy import create_engine

# Establish a database connection
connection = mysql.connector.connect(
    host='localhost',
    database='deelab',
    user='root',
    password='/0OTU5E/',
    port=3306
)
cursor = connection.cursor(buffered=True)
print("Database connected successfully")

# Retrieve data from PRODUCT_PRICE table
query = cursor.execute("SELECT * FROM PRODUCT_PRICE")
df2 = pd.DataFrame(cursor.fetchall(), columns=['PRODUCT_ID', 'PRODUCT_NAME', 'PRICE'])
df2.head()

# Delete a row from PRODUCT_PRICE
query = cursor.execute("DELETE FROM PRODUCT_PRICE WHERE PRODUCT_ID = 2532")
query = cursor.execute("COMMIT")

# Retrieve updated data from PRODUCT_PRICE
query = cursor.execute("SELECT * FROM PRODUCT_PRICE")
df3 = pd.DataFrame(cursor.fetchall(), columns=['PRODUCT_ID', 'PRODUCT_NAME', 'PRICE'])
df3.head()

# Update PRICE to empty string where PRODUCT_ID is 3476
query = cursor.execute("UPDATE PRODUCT_PRICE SET PRICE = '' WHERE PRODUCT_ID = 3476")

# Retrieve updated data from PRODUCT_PRICE
query = cursor.execute("SELECT * FROM PRODUCT_PRICE")
df3 = pd.DataFrame(cursor.fetchall(), columns=['PRODUCT_ID', 'PRODUCT_NAME', 'PRICE'])
df3.head()

# Update PRICE to '####' where PRODUCT_NAME is 'FOOL BALL\R'
query = cursor.execute("UPDATE PRODUCT_PRICE SET PRICE = '####' WHERE PRODUCT_NAME = 'FOOL BALL\R'")

# Retrieve updated data from PRODUCT_PRICE
query = cursor.execute("SELECT * FROM PRODUCT_PRICE")
df3 = pd.DataFrame(cursor.fetchall(), columns=['PRODUCT_ID', 'PRODUCT_NAME', 'PRICE'])
df3.head()
