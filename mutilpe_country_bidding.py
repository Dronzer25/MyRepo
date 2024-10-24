from entsoe import EntsoePandasClient
import pandas as pd
import time

# Your API key from ENTSO-E Transparency Platform
api_key = 'cb8d8bb9-c0c6-4856-9b08-100e74d82064'

# Initialize the EntsoePandasClient
client = EntsoePandasClient(api_key=api_key , timeout=60) # 60 seconds  

# Define the list of countries and their respective bidding zones
countries_bidding_zones = [
    {'country': 'Czech Republic', 'bidding_zone': 'CZ'},
    {'country': 'Germany', 'bidding_zone': 'DE_LU'},
    {'country': 'France', 'bidding_zone': 'FR'}
]

# Define the time range in UTC
start_date_utc = pd.Timestamp('2022-01-01 00:00:00', tz='UTC')
end_date_utc = pd.Timestamp('2022-01-01 23:59:59', tz='UTC')

# Function to ensure DataFrame structure
def ensure_dataframe(df):
    if isinstance(df, pd.Series):
        return df.to_frame()
    return df

# Function to rename columns if they are tuples
def rename_columns(df):
    df.columns = [f"{col[0]}_{col[1]}" if isinstance(col, tuple) else col for col in df.columns]
    return df

# Initialize an empty DataFrame to store combined data
combined_all_countries_df = pd.DataFrame()

# Function to fetch and return data for a specific country and bidding zone
def fetch_data_for_country(country, bidding_zone):
    retries = 5  # Number of retries
    for attempt in range(retries):
        try:
            # Fetch data
            generation_data = client.query_generation(bidding_zone, start=start_date_utc, end=end_date_utc)

            # Adding required columns
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

            # Ensure DataFrame structure and reset index
            generation_data = ensure_dataframe(generation_data).reset_index(drop=True)
            generation_forecast = ensure_dataframe(generation_forecast).reset_index(drop=True)
            load_forecast = ensure_dataframe(load_forecast).reset_index(drop=True)
            actual_load = ensure_dataframe(actual_load).reset_index(drop=True)
            day_ahead_prices = ensure_dataframe(day_ahead_prices).reset_index(drop=True)

            # Rename columns
            generation_data = rename_columns(generation_data)
            generation_forecast = rename_columns(generation_forecast)
            load_forecast = rename_columns(load_forecast)
            actual_load = rename_columns(actual_load)
            day_ahead_prices = rename_columns(day_ahead_prices)

            # Create DataFrames for country and bidding zone
            country_df = pd.DataFrame({'country': [country] * len(generation_data)})
            bidding_zone_df = pd.DataFrame({'bidding_zone': [bidding_zone] * len(generation_data)})

            # Combine all DataFrames side by side
            combined_df = pd.concat(
                [country_df, bidding_zone_df, generation_data, generation_forecast, load_forecast, actual_load, day_ahead_prices],
                axis=1
            )

            # Handle timezone column (index_)
            if 'index_' in combined_df.columns:
                combined_df = combined_df.rename(columns={'index_': 'ts'})
                combined_df['ts'] = combined_df['ts'].astype(str).str.replace(r'\+.*', '', regex=True).str.strip()  # Remove '+' and everything after

            # Drop rows with NaT
            combined_df.dropna(inplace=True)

            print(f'Data for {country} ({bidding_zone}) fetched successfully.')
            return combined_df  # Return the combined data for this country

        except Exception as e:
            print(f"An error occurred for {country} ({bidding_zone}): {e}")
            if attempt < retries - 1:
                print(f"Retrying... ({attempt + 1}/{retries})")
                time.sleep(2)  # Wait before retrying

    return pd.DataFrame()  # Return an empty DataFrame in case of failure

# Loop through each country and bidding zone, fetch data, and append to combined DataFrame
for entry in countries_bidding_zones:
    country_data = fetch_data_for_country(entry['country'], entry['bidding_zone'])
    combined_all_countries_df = pd.concat([combined_all_countries_df, country_data], axis=0, ignore_index=True)

# Save the combined DataFrame to a single CSV file
combined_all_countries_df.to_csv('combined_data_all_countries.csv', index=False)
print('Data for all countries saved to combined_data_all_countries.csv')
