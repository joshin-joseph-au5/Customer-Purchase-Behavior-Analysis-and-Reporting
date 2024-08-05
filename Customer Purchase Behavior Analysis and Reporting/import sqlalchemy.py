from sqlalchemy import create_engine
import pandas as pd

# Correct connection parameters
username = 'root'
password = 'joshin@2016'
host = 'localhost'  # or 'localhost'
port = '3306'       # default MySQL port
database = 'joshinmasaiproject'

# Create SQLAlchemy engine
engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}')

# Example query
query = "SELECT * FROM customers"

# Fetch data into DataFrame
df = pd.read_sql_query(query, engine)

# Display DataFrame
print(df.head())
