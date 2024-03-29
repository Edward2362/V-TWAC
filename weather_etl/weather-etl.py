import pandas as pd
import datetime
import time
import os
from pymongo import MongoClient

CLIENT = MongoClient(os.environ.get("MONGO_COMPASS"))
RAW_WEATHER_COLLECTION = CLIENT.raw.weather
CLEAN_WEATHER_COLLECTION = CLIENT.clean.weather


def extract_data(raw_collection, clean_collection):
    try:
        # Extract the latest timestamp from the clean database
        latest_document = clean_collection.find_one(sort=[("ts", -1)])

        # Get the latest timestamp from the clean database
        if latest_document is not None:
            latest_timestamp = pd.to_datetime(latest_document.get('ts')).timestamp()
        else:
            latest_timestamp = None

        # Extract data from the raw database
        if latest_timestamp is None:
            weather_raw_df = pd.DataFrame(list(raw_collection.find()))
        else:
            weather_raw_df = pd.DataFrame(list(raw_collection.find({'ts': {'$gt': latest_timestamp}})))
            
        # Check if there is new data to process
        if weather_raw_df.empty:
            print('No new data to process')
            return None
        
        return weather_raw_df
    except Exception as e:
        print(e)
        return None


def transform_data(weather_raw_df): 
    # flatten column
    weather_flattened = pd.json_normalize(weather_raw_df['weather']).add_prefix('weather_')
    
    flattened_weather_df = pd.concat([weather_raw_df, weather_flattened], axis=1)

    # drop unnecessary columns
    flattened_weather_df.drop(columns=['weather'], inplace=True)
    
    # convert ts column to datetime format
    flattened_weather_df['ts'] = pd.to_datetime(flattened_weather_df['ts'], unit='s')

    return flattened_weather_df


def load_data(df, collection):
    weather_data = df.to_dict(orient='records')
    collection.insert_many(weather_data)
    
# sleep and show count until the next time the api will be call again
def pause():
    next_time = datetime.datetime.now().replace(hour=16, minute=30, second=0) + datetime.timedelta(days=1)
    print(f"==========================================================================================================")
    print(f"Next API call at: {next_time}")

    while (datetime.datetime.now()) < next_time:
        current = next_time - datetime.datetime.now()
        print(f"   {current.seconds} seconds left to next call")
        time.sleep(60)

def run():
    while True:
        pause()
        # Extract data from the raw databse
        weather_raw_df = extract_data(RAW_WEATHER_COLLECTION, CLEAN_WEATHER_COLLECTION)
        print(weather_raw_df)

        if weather_raw_df is not None:
            weather_df = transform_data(weather_raw_df)
            load_data(weather_df, CLEAN_WEATHER_COLLECTION)
    

if __name__ == "__main__":
    run()
