import requests
import asyncio
import pymongo
import time
import os
from pymongo import UpdateOne

SLEEP_TIME = int(os.environ.get("SLEEP_TIME", 1800))
CLIENT = pymongo.MongoClient(os.environ.get("MONGO_COMPASS"))
RAW_DB = CLIENT.raw
TRAFFIC_COLLECTION = RAW_DB.traffic


async def get_incidents(minLon, minLat, maxLon, maxLat):
    traffic = (
        "https://api.tomtom.com/traffic/services/5/incidentDetails?key=xbOzQloQp2Z0WBkYQaeencnEpPsXJMTU&fields={incidents{type,geometry{type,coordinates},properties{id,iconCategory,magnitudeOfDelay,events{description,code,iconCategory},startTime,endTime,from,to,length,delay,roadNumbers,timeValidity,probabilityOfOccurrence,numberOfReports,lastReportTime}}}&language=en-GB&t=1111&timeValidityFilter=present&"
        + f"bbox={minLon},{minLat},{maxLon},{maxLat}&categoryFilter=1,6"
    )
    res = requests.get(traffic)
    return res.json()["incidents"]


def get_upsert_incident_requests(documents):
    update_requests = []
    for document in documents:
        document["_id"] = document["properties"]["id"]
        update_requests.append(
            UpdateOne({"_id": document["_id"]}, {"$set": document}, upsert=True)
        )

    return update_requests


def run():
    cities_bboxes = {
        "Hanoi": [105.2849, 20.5645, 106.0201, 21.3853],
        "Hai Phong": [106.4005, 20.2208, 107.1187, 21.0203],
        "Da Nang": [107.818782, 15.917955, 108.574858, 16.344307],
        "HCM City": [106.532129, 10.66594, 106.831575, 10.883411],
        "Can Tho": [105.225678, 9.919531, 105.845472, 10.325209],
    }
    iterator = 0
    repeat_request = SLEEP_TIME / len(cities_bboxes)
    cities = list(cities_bboxes.keys())

    while True:
        city = cities[iterator % len(cities)]
        incidents = asyncio.run(
            get_incidents(
                cities_bboxes.get(city)[0],
                cities_bboxes.get(city)[1],
                cities_bboxes.get(city)[2],
                cities_bboxes.get(city)[3],
            )
        )
        upsert_incident_requests = get_upsert_incident_requests(incidents)
        print(f"Sending new incidents in {city} to raw db\n")
        if (len(upsert_incident_requests) > 0):
            TRAFFIC_COLLECTION.bulk_write(upsert_incident_requests)
            print("New incidents sent\n")
            time.sleep(repeat_request)
            print("Waking up!\n")
            
        else:
            print(f"No new incidents in {city}")
        iterator += 1

if __name__ == "__main__":
    run()
