import sqlite3
import pandas as pd


db_path = "Data Engineer_ETL Assignment 

# Output CSV path
output_csv = "item_sales_by_customer.csv"

# Connecting to SQLite DB
conn = sqlite3.connect(db_path)

# --------------------------
# Solution 1: Pure SQL
# --------------------------
sql_query = """
SELECT
    c.customer_id AS Customer,
    c.age AS Age,
    i.item_name AS Item,
    SUM(o.quantity) AS Quantity
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
JOIN orders o ON s.sales_id = o.sales_id
JOIN items i ON o.item_id = i.item_id
WHERE c.age BETWEEN 18 AND 35
  AND o.quantity IS NOT NULL
GROUP BY c.customer_id, c.age, i.item_name
HAVING SUM(o.quantity) > 0
ORDER BY c.customer_id, i.item_name;
"""

df_sql = pd.read_sql_query(sql_query, conn)

# --------------------------
# Solution 2: Using Pandas
# --------------------------
# Load tables into DataFrames
df_customer = pd.read_sql_query("SELECT * FROM customers", conn)
df_sales = pd.read_sql_query("SELECT * FROM sales", conn)
df_orders = pd.read_sql_query("SELECT * FROM orders", conn)
df_items = pd.read_sql_query("SELECT * FROM items", conn)

df = (
    df_sales.merge(df_customer, on="customer_id")
            .merge(df_orders, on="sales_id")
            .merge(df_items, on="item_id")
)

df_filtered = df[(df['age'].between(18, 35)) & (df['quantity'].notnull())]

df_result = (
    df_filtered.groupby(['customer_id', 'age', 'item_name'], as_index=False)
               .agg({'quantity': 'sum'})
)

df_result = df_result[df_result['quantity'] > 0]

df_result.columns = ['Customer', 'Age', 'Item', 'Quantity']

df_result['Quantity'] = df_result['Quantity'].astype(int)


df_result.to_csv(output_csv, sep=';', index=False)

print(f"Report saved to: {output_csv}")