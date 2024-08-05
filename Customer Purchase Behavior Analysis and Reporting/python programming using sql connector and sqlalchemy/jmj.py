from sqlalchemy import create_engine, text
import pandas as pd

# URL-encode the password
password = "joshin%402016"

# Establish connection to the database
engine = create_engine(f"mysql+mysqlconnector://root:{password}@localhost/joshinmasaiproject")

def calculate_total_purchases(connection):
    query = text("SELECT COUNT(DISTINCT TransactionID) AS total_purchases FROM transactions;")
    result = connection.execute(query)
    total_purchases = result.scalar()
    return total_purchases

def calculate_total_revenue(connection):
    query = text("SELECT SUM(PurchaseQuantity * PurchasePrice) AS total_revenue FROM transactions;")
    result = connection.execute(query)
    total_revenue = result.scalar()
    return total_revenue

def calculate_average_purchase_value(total_revenue, total_purchases):
    return total_revenue / total_purchases

def get_top_customers(connection):
    query = text("""
        SELECT c.CustomerID, c.CustomerName, SUM(t.PurchaseQuantity * t.PurchasePrice) AS total_revenue
        FROM transactions t
        JOIN customers c ON t.CustomerID = c.CustomerID
        GROUP BY c.CustomerID, c.CustomerName
        ORDER BY total_revenue DESC
        LIMIT 10;
    """)
    result = connection.execute(query)
    top_customers = pd.DataFrame(result.fetchall(), columns=['CustomerID', 'CustomerName', 'total_revenue'])
    return top_customers

def get_monthly_trends(connection):
    query = text("""
        SELECT YEAR(PurchaseDate) AS year, MONTH(PurchaseDate) AS month, 
               SUM(PurchaseQuantity * PurchasePrice) AS total_revenue
        FROM transactions
        GROUP BY year, month
        ORDER BY year, month;
    """)
    result = connection.execute(query)
    monthly_trends = pd.DataFrame(result.fetchall(), columns=['year', 'month', 'total_revenue'])
    return monthly_trends

def get_quarterly_trends(connection):
    query = text("""
        SELECT YEAR(PurchaseDate) AS year, QUARTER(PurchaseDate) AS quarter, 
               SUM(PurchaseQuantity * PurchasePrice) AS total_revenue
        FROM transactions
        GROUP BY year, quarter
        ORDER BY year, quarter;
    """)
    result = connection.execute(query)
    quarterly_trends = pd.DataFrame(result.fetchall(), columns=['year', 'quarter', 'total_revenue'])
    return quarterly_trends

def get_yearly_trends(connection):
    query = text("""
        SELECT YEAR(PurchaseDate) AS year, 
               SUM(PurchaseQuantity * PurchasePrice) AS total_revenue
        FROM transactions
        GROUP BY year
        ORDER BY year;
    """)
    result = connection.execute(query)
    yearly_trends = pd.DataFrame(result.fetchall(), columns=['year', 'total_revenue'])
    return yearly_trends

def get_top_categories(connection):
    query = text("""
        SELECT p.ProductCategory, SUM(t.PurchaseQuantity * t.PurchasePrice) AS total_revenue
        FROM transactions t
        JOIN products p ON t.ProductID = p.ProductID
        GROUP BY p.ProductCategory
        ORDER BY total_revenue DESC
        LIMIT 10;
    """)
    result = connection.execute(query)
    top_categories = pd.DataFrame(result.fetchall(), columns=['ProductCategory', 'total_revenue'])
    return top_categories

# Main script
def main():
    with engine.connect() as connection:
        total_purchases = calculate_total_purchases(connection)
        total_revenue = calculate_total_revenue(connection)
        average_purchase_value = calculate_average_purchase_value(total_revenue, total_purchases)

        print(f"Total Purchases: {total_purchases}")
        print(f"Total Revenue: {total_revenue}")
        print(f"Average Purchase Value: {average_purchase_value}")

        top_customers = get_top_customers(connection)
        print("Top Customers and Their Purchasing Behavior:")
        print(top_customers)

        monthly_trends = get_monthly_trends(connection)
        print("Monthly Trends:")
        print(monthly_trends)

        quarterly_trends = get_quarterly_trends(connection)
        print("Quarterly Trends:")
        print(quarterly_trends)

        yearly_trends = get_yearly_trends(connection)
        print("Yearly Trends:")
        print(yearly_trends)

        top_categories = get_top_categories(connection)
        print("Top-Performing Product Categories:")
        print(top_categories)

        generate_summary_report(total_purchases, total_revenue, average_purchase_value, top_customers, monthly_trends, quarterly_trends, yearly_trends, top_categories)

# Summary Report with Key Insights
def generate_summary_report(total_purchases, total_revenue, average_purchase_value, top_customers, monthly_trends, quarterly_trends, yearly_trends, top_categories):
    summary_report = {
        "Total Purchases": total_purchases,
        "Total Revenue": total_revenue,
        "Average Purchase Value": average_purchase_value,
        "Top Customers": top_customers,
        "Monthly Trends": monthly_trends,
        "Quarterly Trends": quarterly_trends,
        "Yearly Trends": yearly_trends,
        "Top-Performing Product Categories": top_categories
    }

    print("Summary Report with Key Insights:")
    for key, value in summary_report.items():
        print(f"{key}:")
        print(value)
        print("\n")

# Run the main script
if __name__ == "__main__":
    main()

# Close the engine connection
engine.dispose()
