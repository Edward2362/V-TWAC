import datetime
import time
import pandas as pd
import requests
import pymongo
import os

os.system('pip install pandas==1.4.4')

# Check README.md for more details
server_uri = "MONGODB_URI"
ACCESS_TOKEN = "WEATHERBIT_TOKEN"
client = pymongo.MongoClient(server_uri)
db_name = "forecast"
collection_name = "weather"
collection_results =  client[db_name].results

collection = client[db_name][collection_name]
collection.drop()
collection_results.drop()


def get_weather(coor):
    lat, lon = coor

    api_url = f"https://api.weatherbit.io/v2.0/forecast/hourly?lat={lat}&lon={lon}&key={ACCESS_TOKEN}&hours=240"

    response = requests.get(api_url)
    result = response.json()
    return result['data']


def transform_data(weather_raw_df):
    # flatten column
    weather_flattened = pd.json_normalize(weather_raw_df['weather']).add_prefix('weather_')

    flattened_weather_df = pd.concat([weather_raw_df, weather_flattened], axis=1)

    # drop unnecessary columns
    flattened_weather_df.drop(columns=['weather'], inplace=True)

    # convert ts column to datetime format
    flattened_weather_df['ts'] = pd.to_datetime(flattened_weather_df['ts'], unit='s')

    return flattened_weather_df


def insert_data(city):
    city_coor = {
        "Ha Noi": (21.0294498, 105.8544441),
        "Hai Phong": (20.858864, 106.6749591),
        "Da Nang": (16.068, 108.212),
        "Ho Chi Minh City": (10.7758439, 106.7017555),
        "Can Tho": (10.0364216, 105.7875219)
    }

    result_arr = []
    response = get_weather(city_coor[city])
    for report in response:
        report['location'] = city
        result_arr.append(report)
        # print(report)

    report_df = pd.DataFrame(result_arr)
    report_df = transform_data(report_df)

    return report_df


def upsert(documents):
    update_requests = []
    for document in documents:
        collection.update_one({"ts": document["ts"], 'location': document["location"]}, {"$set": document}, upsert=True)


def append(district, city):
    df = insert_data(city)
    df = df.drop(["ozone", "clouds_low", "clouds_mid", "clouds_hi", 'temp', "wind_cdir_full", "wind_cdir"], axis=1)
    df['district'] = district
    upsert(df.to_dict(orient='records'))
    return df.to_dict(orient='records')

def writeToCollection(dataframe):
    df = dataframe
    df.columns = df.columns.astype(str)

    records = df.to_dict(orient='records')
    collection_results.drop()
    collection_results.insert_many(records)

# if __name__ == '__main__':
#     append('cau giay', 'Ha Noi')

