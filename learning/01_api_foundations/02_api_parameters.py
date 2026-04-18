# API Parameters Exercise Code

import requests
import pandas as pd
from time import sleep

# Define a list of parameters to test
parameters = [
    {'symbol': 'AAPL', 'range': '1d'},
    {'symbol': 'GOOGL', 'range': '5d'},
    {'symbol': 'MSFT', 'range': '1mo'},
]

# Function to fetch API data

def fetch_data(params):
    base_url = 'https://api.example.com/data'  # Replace with actual API endpoint
    response = requests.get(base_url, params=params)
    # Check if the request was successful
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data: {response.status_code}")
        return None

# Dictionary to hold results
results = {}

# Loop through parameters and fetch data
for param in parameters:
    print(f"Fetching data for {param['symbol']} with range {param['range']}...")
    data = fetch_data(param)
    if data:
        results[param['symbol']] = data
    sleep(1)  # Avoid hitting the API too fast by adding delay

# Convert results to a DataFrame for analysis
results_df = pd.DataFrame.from_dict(results, orient='index')

# Print results
print(results_df)

# You can extend the analysis as needed.
