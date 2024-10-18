from entsoe import EntsoePandasClient
import pandas as pd
import time

# Your API key from ENTSO-E Transparency Platform
api_key = 'cb8d8bb9-c0c6-4856-9b08-100e74d82064'

# Initialize the EntsoePandasClient
client = EntsoePandasClient(api_key=api_key)

# Define the bidding zone (Czech Republic - CZ)
bidding_zone = 'CZ'
country = 'Czech Republic'

# Define the time range in Brussels time (CET/CEST)
start_date = pd.Timestamp('2024-01-01 00:00:00', tz='Europe/Brussels')
end_date = pd.Timestamp('2024-01-01 23:59:59', tz='Europe/Brussels')

# Convert to UTC (ENTSO-E requires UTC)
start_date_utc = start_date.tz_convert('UTC')
end_date_utc = end_date.tz_convert('UTC')


# Fetch and print the data with retries
def fetch_and_save_data():
    retries = 5  # Number of retries
    for attempt in range(retries):
        try:
            # Fetch data
            generation_data = client.query_generation(bidding_zone, start=start_date_utc, end=end_date_utc)

            #Adding 3 Column which is required 
            generation_data['Total_generation_actual_aggregated'] = generation_data.filter(like='Aggregated').sum(axis=1)
            generation_data['Total_generation_actual_consumption'] = generation_data.filter(like='Consumption').sum(axis=1)
            generation_data['Total_generation_actual_aggregated_and_consumption'] = (
                generation_data['Total_generation_actual_aggregated'] + generation_data['Total_generation_actual_consumption']
            )

            # Reset the index and drop the second row (index 1)
            generation_data = generation_data.reset_index().drop(index=1)

            generation_forecast = client.query_generation_forecast(bidding_zone, start=start_date_utc, end=end_date_utc)
            load_forecast = client.query_load_forecast(bidding_zone, start=start_date_utc, end=end_date_utc)
            actual_load = client.query_load(bidding_zone, start=start_date_utc, end=end_date_utc)
            day_ahead_prices = client.query_day_ahead_prices(bidding_zone, start=start_date_utc, end=end_date_utc)
            
            #Only for day_ahead price as its column name is getting different 
            # # Check if day_ahead_prices is a Series, convert to DataFrame if necessary
            if isinstance(day_ahead_prices, pd.Series):
                day_ahead_prices = day_ahead_prices.to_frame()

            # Check if column '0' exists, then rename it to 'DAY_AHEAD_PRICE'
            if 0 in day_ahead_prices.columns:
                day_ahead_prices.rename(columns={0: 'day_ahead_price'}, inplace=True)

            print('Fetching is Done')
            
            # Function to ensure DataFrame structure
            def ensure_dataframe(df):
                if isinstance(df, pd.Series):
                    return df.to_frame()
                return df
            
            # Reset index to keep time as a column only for generation_data
            generation_data = generation_data.reset_index(drop=True)
            
            generation_forecast = ensure_dataframe(generation_forecast).reset_index(drop=True)  # Drop time index
            load_forecast = ensure_dataframe(load_forecast).reset_index(drop=True)  # Drop time index
            actual_load = ensure_dataframe(actual_load).reset_index(drop=True)  # Drop time index
            day_ahead_prices = ensure_dataframe(day_ahead_prices).reset_index(drop=True)  # Drop time index
            
            # Function to rename columns if they are tuples
            def rename_columns(df):
                df.columns = [f"{col[0]}_{col[1]}" if isinstance(col, tuple) else col for col in df.columns]
                return df

            # Use the rename function in your main code
            generation_data = rename_columns(generation_data)
            generation_forecast = rename_columns(generation_forecast)
            load_forecast = rename_columns(load_forecast)
            actual_load = rename_columns(actual_load)
            day_ahead_prices = rename_columns(day_ahead_prices)

            
            # Create 2 df for country and bidding zone
            country_df = pd.DataFrame({'country': [country] * len(generation_data)})
            bidding_zone_df = pd.DataFrame({'bidding_zone': [bidding_zone] * len(generation_data)})
            
            # Combine all DataFrames side by side
            combined_df = pd.concat(
                [country_df, bidding_zone_df, generation_data, generation_forecast, load_forecast, actual_load, day_ahead_prices],
                axis=1
            )

            #FOR TIMEZONE which was getting set as index_ 
            if 'index_' in combined_df.columns:
                combined_df = combined_df.rename(columns={'index_': 'ts'})
                combined_df['ts'] = combined_df['ts'].astype(str).str.replace(r'\+.*', '', regex=True).str.strip()  # Remove '+' and everything after
            
            # Drop any rows with NaT in any column
            combined_df.dropna(inplace=True)

            # Save combined DataFrame to CSV
            combined_df.to_csv('SujataL.csv', index=False)

            print('Data saved to final_data_new_1.csv')
            break  # Exit the retry loop if successful

        except Exception as e:
            print(f"An error occurred: {e}")
            if attempt < retries - 1:
                print(f"Retrying... ({attempt + 1}/{retries})")
                time.sleep(2)  # Wait before retrying

# Fetch data and save to CSV
fetch_and_save_data()
