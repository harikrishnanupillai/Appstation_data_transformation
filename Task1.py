import pandas as pd
import json

# Load the JSON data
with open('sales_data.json', 'r') as file:
    data = json.load(file)
# Convert JSON to DataFrame
df = pd.DataFrame(data)

# Flatten the nested product object into columns
df = pd.concat([df.drop(['product'], axis=1), df['product'].apply(pd.Series)], axis=1)

# Rename product columns for clarity
df.rename(columns={'id': 'product_id', 'name': 'product_name'}, inplace=True)

df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%dT%H:%M:%S%z', errors='coerce')

# For rows where the ISO format failed, try parsing with the simple YYYY-MM-DD format
df['date'] = df['date'].fillna(pd.to_datetime(df['date'], format='%Y-%m-%d', errors='coerce'))

# Drop rows with invalid dates (if any)
df = df.dropna(subset=['date'])

# Format the date to YYYY-MM-DD
df['date'] = df['date'].dt.strftime('%Y-%m-%d')

# Calculate total_value (quantity * price)
df['total_value'] = df['quantity'] * df['price']

# Handle missing or invalid data
# Replace null customer_id with 'Unknown'
df['customer_id'].fillna('Unknown', inplace=True)

# Handle negative quantities (Replace negative quqntities with 0)
df['quantity'] = df['quantity'].apply(lambda x: max(0, x))

# Save the processed data to a new CSV file if data loading to Postgresql is via CSV file
df.to_csv('processed_sales_data.csv', index=False)

# Display the processed DataFrame
print(df.head())