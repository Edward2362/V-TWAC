import datetime
import time
import requests
import pymongo
from dotenv import load_dotenv
import os

load_dotenv()

server_uri = os.getenv('MONGO_URI')
ACCESS_TOKEN = os.getenv('WB_TOKEN')
client = pymongo.MongoClient(server_uri)
db_name = "raw"
collection_name = "weather"

collection = client[db_name][collection_name]


def get_weather(coor):
    lat, lon = coor
    today = datetime.datetime.now().replace(hour=16, minute=0, second=0) - datetime.timedelta(hours=2)
    yesterday = today - datetime.timedelta(days=1)

    api_url = f"https://api.weatherbit.io/v2.0/history/hourly?lat={lat}&lon={lon}&start_date={yesterday.strftime('%Y-%m-%d:%H')}&end_date={today.strftime('%Y-%m-%d:%H')}&tz=local&key={ACCESS_TOKEN}"
    
    response = requests.get(api_url)
    result = response.json()
    print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    print(result)
    return result['data']


def insert_data():
    city_coor = {
        "Ha Noi": (21.0294498, 105.8544441),
        "Hai Phong": (20.858864, 106.6749591),
        "Da Nang": (16.068, 108.212),
        "Ho Chi Minh City": (10.7758439, 106.7017555),
        "Can Tho": (10.0364216, 105.7875219)
    }

    for city in city_coor:
        response = get_weather(city_coor[city])
        for report in response:
            report['location'] = city
            print(report)
            collection.insert_one(report)


def pause():
    next_time = datetime.datetime.now().replace(hour=16, minute=0, second=0) + datetime.timedelta(days=1)
    print(f"==========================================================================================================")
    print(f"Next API call at: {next_time}")

    while (datetime.datetime.now()) < next_time:
        current = next_time - datetime.datetime.now()
        print(f"   {current.seconds} seconds left to next call")
        time.sleep(60)


def run():
    while True:
        pause()
        insert_data()


if __name__ == "__main__":
    run()
