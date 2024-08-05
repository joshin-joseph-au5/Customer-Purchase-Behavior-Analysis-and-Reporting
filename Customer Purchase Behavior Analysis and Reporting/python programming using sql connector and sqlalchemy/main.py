from sqlalchemy import create_engine, text
import urllib.parse
import pandas as pd

# Database credentials
user = 'root'
password = 'joshin@2016'
host = 'localhost'
port = 3306
database = 'joshinmasaiproject'

# Encode the password
password_encoded = urllib.parse.quote_plus(password)

# Create the connection string
connection_string = f'mysql+pymysql://{user}:{password_encoded}@{host}:{port}/{database}'

# Create the database engine
engine = create_engine(connection_string)

# Extract data into DataFrames
customers_df = pd.read_sql("SELECT * FROM customers", engine)
products_df = pd.read_sql("SELECT * FROM products", engine)
transactions_df = pd.read_sql("SELECT * FROM transactions", engine)

total_purchases = transactions_df['TransactionID'].nunique()
total_revenue = (transactions_df['PurchaseQuantity'] * transactions_df['PurchasePrice']).sum()
average_purchase_value = total_revenue / total_purchases

print(f"Total Purchases: {total_purchases}")
print(f"Total Revenue: ${total_revenue:.2f}")
print(f"Average Purchase Value: ${average_purchase_value:.2f}")


top_customers = transactions_df.groupby('CustomerID').agg({
    'TransactionID': 'count',
    'PurchaseQuantity': 'sum',
    'PurchasePrice': 'sum'
}).rename(columns={'TransactionID': 'total_transactions', 'PurchaseQuantity': 'total_quantity', 'PurchasePrice': 'total_spent'}).sort_values(by='total_spent', ascending=False).head(10)

top_customers = top_customers.merge(customers_df, on='CustomerID')
print("Top Customers:")
print(top_customers)


transactions_df['PurchaseDate'] = pd.to_datetime(transactions_df['PurchaseDate'])
transactions_df['month'] = transactions_df['PurchaseDate'].dt.to_period('M')
transactions_df['quarter'] = transactions_df['PurchaseDate'].dt.to_period('Q')
transactions_df['year'] = transactions_df['PurchaseDate'].dt.to_period('Y')

# Monthly trends
monthly_trends = transactions_df.groupby('month').agg({
    'TransactionID': 'count',
    'PurchaseQuantity': 'sum',
    'PurchasePrice': 'sum'
}).rename(columns={'TransactionID': 'total_transactions', 'PurchaseQuantity': 'total_quantity', 'PurchasePrice': 'total_revenue'})

# Quarterly trends
quarterly_trends = transactions_df.groupby('quarter').agg({
    'TransactionID': 'count',
    'PurchaseQuantity': 'sum',
    'PurchasePrice': 'sum'
}).rename(columns={'TransactionID': 'total_transactions', 'PurchaseQuantity': 'total_quantity', 'PurchasePrice': 'total_revenue'})

# Yearly trends
yearly_trends = transactions_df.groupby('year').agg({
    'TransactionID': 'count',
    'PurchaseQuantity': 'sum',
    'PurchasePrice': 'sum'
}).rename(columns={'TransactionID': 'total_transactions', 'PurchaseQuantity': 'total_quantity', 'PurchasePrice': 'total_revenue'})

print("Monthly Trends:")
print(monthly_trends)

print("Quarterly Trends:")
print(quarterly_trends)

print("Yearly Trends:")
print(yearly_trends)
