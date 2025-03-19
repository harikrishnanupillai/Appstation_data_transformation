import pandas as pd
import json
import logging

# Set up logging to log anomalies to a file
logging.basicConfig(filename='data_processing_anomalies.log', level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Load the JSON data
with open('sales_data.json', 'r') as file:
    data = json.load(file)

# Convert JSON to DataFrame
df = pd.DataFrame(data)

# 1. Validate JSON data for missing fields 
df['customer_id'].fillna('Unknown', inplace=True)
logging.info(f"Replaced {df['customer_id'].isnull().sum()} null customer_id values with 'Unknown'.")

# 2. Handle negative quantity values 
negative_quantity_count = (df['quantity'] < 0).sum()
df.loc[df['quantity'] < 0, 'quantity'] = 0  # Set negative quantities to 0
logging.info(f"Found and corrected {negative_quantity_count} negative quantity values.")

# 3. Ensure no duplicate transaction_id entries are loaded
duplicate_transactions = df[df.duplicated('transaction_id', keep=False)]
if not duplicate_transactions.empty:
    logging.warning(f"Found {len(duplicate_transactions)} duplicate transaction_id entries. Removing duplicates.")
    df.drop_duplicates(subset=['transaction_id'], keep='first', inplace=True)

# 4. Log anomalies during processing
# Log rows with missing or invalid data
missing_data = df[df.isnull().any(axis=1)]
if not missing_data.empty:
    logging.warning(f"Found {len(missing_data)} rows with missing data:\n{missing_data}")

# Log rows with negative quantities (before correction)
negative_quantity_rows = df[df['quantity'] < 0]
if not negative_quantity_rows.empty:
    logging.warning(f"Found {len(negative_quantity_rows)} rows with negative quantities:\n{negative_quantity_rows}")

# Log duplicate transaction_id entries (before removal)
if not duplicate_transactions.empty:
    logging.warning(f"Duplicate transaction_id entries:\n{duplicate_transactions}")

# Save the processed data to a new CSV file
df.to_csv('processed_sales_data.csv', index=False)

# Log completion of processing
logging.info("Data processing completed. Processed data saved to 'processed_sales_data.csv'.")

# Print summary to console if needed
print(f"Processed data summary:")
print(f"- Replaced null customer_id values: {df['customer_id'].isnull().sum()}")
print(f"- Corrected negative quantities: {negative_quantity_count}")
print(f"- Removed duplicate transaction_id entries: {len(duplicate_transactions)}")
print(f"- Rows with missing data: {len(missing_data)}")
print(f"Processed data saved to 'processed_sales_data.csv'.")