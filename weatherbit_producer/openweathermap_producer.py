import datetime
import time
import requests
import pymongo
from dotenv import load_dotenv
import os

load_dotenv()

server_uri = os.getenv('MONGO_URI')
ACCESS_TOKEN = os.getenv('OWM_TOKEN')
client = pymongo.MongoClient(server_uri)
db_name = "raw"
collection_name = "weather"

collection = client[db_name][collection_name]


def get_weather(area):
    api_url = "https://history.openweathermap.org/data/2.5/history/city?lat={lat}&lon={lon}&appid={token}"
    geo_url = "http://api.openweathermap.org/geo/1.0/direct?q={area},VN&limit=1&appid={token}"

    coordinate = requests.get(geo_url.format(area=area, token=ACCESS_TOKEN)).json()[0]
    response = requests.get(api_url.format(lat=coordinate['lat'], lon=coordinate['lon'], token=ACCESS_TOKEN))
    result = response.json()
    print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    print(area + ", lat: " + str(coordinate['lat']) + ", lon: " + str(coordinate['lon']) + '\n')

    return result['list']


def insert_data():
    city_arr = [
        "Ha Noi", "Hai Phong", "Da Nang", "Ho Chi Minh City", "Can Tho"
    ] 

    for name in city_arr:
        response = get_weather(name)
        for report in response:
            report['location'] = name
            print(report)
            collection.insert_one(report)


def pause():
    next_time = datetime.datetime.now().replace(second=0) + datetime.timedelta(days=1)
    print(f"==========================================================================================================")
    print(f"Next API call at: {next_time}")

    while (datetime.datetime.now()) < next_time:
        current = next_time - datetime.datetime.now()
        print(f"   {current.seconds} seconds left to next call")
        time.sleep(60)


def run():
    while True:
        insert_data()
        pause()


if __name__ == "__main__":
    run()