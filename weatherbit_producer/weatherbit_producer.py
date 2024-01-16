import datetime
import time
import requests
import pymongo
from dotenv import load_dotenv
import os

load_dotenv()

#get varible from .env file
SERVER_URI = os.getenv('MONGO_URI')
ACCESS_TOKEN = os.getenv('WB_TOKEN')

CLIENT = pymongo.MongoClient(SERVER_URI)

COLLECTION = CLIENT.raw.weather

# using lat and lon in coor put in to the api and call the request
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

# loop through 5 city and call get_weather then insert into raw weathehr collection
def insert_data():
    # 5 cities with it coordinates
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
            # pack the report with the city location then insert into raw weather
            report['location'] = city
            print(report)
            COLLECTION.insert_one(report)

# sleep and show count until the next time the api will be call again
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
