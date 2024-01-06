import datetime
import time

import requests
import pymongo


server_uri = "mongodb+srv://admin:PW0ysJYzBz2MTCCt@hd.4znyikg.mongodb.net/"
client = pymongo.MongoClient(server_uri)
db_name = "raw"
collection_name = "weather"

collection = client[db_name][collection_name]


def get_weather(area):
    ACCESS_TOKEN = "7a2bc70137a9acce1f36f307f8ebd2f8"
    api_url = "https://history.openweathermap.org/data/2.5/history/city?lat={lat}&lon={lon}&appid={token}"
    geo_url = "http://api.openweathermap.org/geo/1.0/direct?q={area},VN&limit=1&appid={token}"

    coordinate = requests.get(geo_url.format(area=area, token=ACCESS_TOKEN)).json()[0]
    response = requests.get(api_url.format(lat=coordinate['lat'], lon=coordinate['lon'], token=ACCESS_TOKEN))
    result = response.json()
    print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    print(area + ", lat: " + str(coordinate['lat']) + ", lon: " + str(coordinate['lon']) + '\n')

    return result['list']


def insert_data():
    # provinces_and_cities_arr = [
    #     "An Giang", "Ba Ria-Vung Tau", "Bac Lieu", "Bac Kan", "Bac Giang",
    #     "Bac Ninh", "Ben Tre", "Binh Duong", "Binh Dinh", "Binh Phuoc",
    #     "Binh Thuan", "Ca Mau", "Cao Bang", "Can Tho", "Da Nang",
    #     "Dak Lak", "Dak Nong", "Dien Bien", "Dong Nai", "Dong Thap",
    #     "Gia Lai", "Ha Giang", "Ha Nam", "Ha Noi", "Ha Tinh",
    #     "Hai Duong", "Hai Phong", "Hau Giang", "Ho Chi Minh City", "Hoa Binh",
    #     "Hung Yen", "Khanh Hoa", "Kien Giang", "Kon Tum", "Lai Chau",
    #     "Lang Son", "Lao Cai", "Lam Dong", "Long An", "Nam Dinh",
    #     "Nghe An", "Ninh Binh", "Ninh Thuan", "Phu Tho", "Phu Yen",
    #     "Quang Binh", "Quang Nam", "Quang Ngai", "Quang Ninh", "Quang Tri",
    #     "Soc Trang", "Son La", "Tay Ninh", "Thai Binh", "Thai Nguyen",
    #     "Thanh Hoa", "Thua Thien Hue", "Tien Giang", "Tra Vinh", "Tuyen Quang",
    #     "Vinh Long", "Vinh Phuc", "Yen Bai"
    # ]

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
    # print(get_weather("Hai Phong"))
