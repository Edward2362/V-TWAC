import pandas as pd
import time
import os
from pymongo import MongoClient

SLEEP_TIME = int(os.environ.get("SLEEP_TIME", 86400))
CLIENT = MongoClient(os.environ.get("MONGO_COMPASS"))
RAW_WEATHER_COLLECTION = CLIENT.raw.weather
CLEAN_WEATHER_COLLECTION = CLIENT.clean.weather


def extract_data(raw_collection, clean_collection):
    try:
        # Extract the latest timestamp from the clean database
        latest_document = clean_collection.find_one(sort=[("dt", -1)])

        if latest_document is not None:
            latest_timestamp = pd.to_datetime(latest_document.get('dt')).timestamp()
        else:
            latest_timestamp = None

        if latest_timestamp is None:
            weather_raw_df = pd.DataFrame(list(raw_collection.find()))
        else:
            weather_raw_df = pd.DataFrame(list(raw_collection.find({'dt': {'$gt': latest_timestamp}})))
            
        if weather_raw_df.empty:
            print('No new data to process')
            return None
        
        return weather_raw_df
    except Exception as e:
        print(e)
        return None


def transform_data(weather_raw_df): 
    # flatten column
    main_flattened = pd.json_normalize(weather_raw_df['main']).add_prefix('main_')
    wind_flattened = pd.json_normalize(weather_raw_df['wind']).add_prefix('wind_')
    clouds_flattened = pd.json_normalize(weather_raw_df['clouds']).add_prefix('clouds_')
    weather_flattened = pd.json_normalize(weather_raw_df['weather']).add_prefix('weather_')
    
    flattened_weather_df = pd.concat([weather_raw_df, main_flattened, wind_flattened, clouds_flattened, weather_flattened], axis=1)

    # get max length of weather column
    max_weather_arr_len = weather_raw_df['weather'].apply(lambda x: len(x) if isinstance(x, list) else 0).max()
    weather_arr = []
    for i in range(max_weather_arr_len):
        weather_arr.append(f'weather_{i}')
    
    # flatten the dictionary inside weather column
    for weather in weather_arr:
        flattened_column = pd.json_normalize(flattened_weather_df[weather]).add_prefix(f'{weather}_')
        flattened_weather_df = pd.concat([flattened_weather_df, flattened_column], axis=1)
        
    cols_to_drop = ['main', 'wind', 'clouds', 'weather'] + weather_arr

    # drop unnecessary columns
    flattened_weather_df.drop(columns=cols_to_drop, inplace=True)
    
    # convert dt column to datetime format
    flattened_weather_df['dt'] = pd.to_datetime(flattened_weather_df['dt'], unit='s')

    return flattened_weather_df


def load_data(df, collection):
    weather_data = df.to_dict(orient='records')
    collection.insert_many(weather_data)
    

def run():
    while True:
        # Extract data from the raw databse
        weather_raw_df = extract_data(RAW_WEATHER_COLLECTION, CLEAN_WEATHER_COLLECTION)

        if weather_raw_df is not None:
            weather_df = transform_data(weather_raw_df)
            load_data(weather_df, CLEAN_WEATHER_COLLECTION)
        time.sleep(SLEEP_TIME)
    

if __name__ == "__main__":
    run()
