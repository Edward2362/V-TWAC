import pymongo
import time
import os
import multiprocessing
from unidecode import unidecode
from pymongo import DeleteOne, UpdateOne
from geopy.geocoders import Nominatim


SLEEP_TIME = int(os.environ.get("SLEEP_TIME", 21600))
CLIENT = pymongo.MongoClient(os.environ.get("MONGO_COMPASS"))

RAW_DB = CLIENT.raw
TRAFFIC_RAW_COLLECTION = RAW_DB.traffic

CLEAN_DB = CLIENT.clean
TRAFFIC_CLEAN_COLLECTION = CLEAN_DB.traffic

CHECKPOINT_DB = CLIENT.checkpoint
TRAFFIC_CP_COLLECTION = CHECKPOINT_DB.checkpoints

CITES_ISO = {
    "VN-HN": "Ha Noi",
    "VN-HP": "Hai Phong",
    "VN-DN": "Da Nang",
    "VN-SG": "Ho Chi Minh City",
    "VN-CT": "Can Tho",
}


def extract_name(district):
    district_lowercase = district.lower()
    if (
        len(district_lowercase.split()) == 2
        and district_lowercase.split()[1].isnumeric()
    ):
        return district_lowercase
    elif "quận" in district_lowercase:
        return district_lowercase.replace("quận", "").strip()
    return district_lowercase.replace("district", "").strip()


def get_district_key(address):
    result = ""
    for key in address.keys():
        value = address[key].lower()
        if (
            ("district" in value or "quận" in value)
            and "đường" not in value
            and "street" not in value
        ):
            result = key
    return result


def covert_coordinates(coordinates):
    geolocator = Nominatim(user_agent="myapplication", timeout=None)
    x = (coordinates[1], coordinates[0])  # Lat, Lon
    location = geolocator.reverse(
        x, exactly_one=True, language="en", namedetails=True, addressdetails=True
    )
    address = location.raw["address"]
    district_key = get_district_key(address)

    try:
        if (
            "city" in address
            and district_key != ""
            and address["ISO3166-2-lvl4"] in CITES_ISO
        ):
            district = unidecode(extract_name(location.raw["address"][district_key]))
            city = CITES_ISO.get(location.raw["address"]["ISO3166-2-lvl4"])
            return (district, city)
    except:
        print(f"Error in address: {address}")
        return None, None
    return None, None


def clean_raw_data(document):
    district, city = covert_coordinates(document["geometry"]["coordinates"][0])
    if not city:
        return
    cleaned_incident = {
        "_id": document["_id"],
        "iconCategory": document["properties"]["iconCategory"],
        "startTime": document["properties"]["startTime"],
        "endTime": document["properties"]["endTime"],
        "length": document["properties"]["length"],
        "delay": document["properties"]["delay"],
        "eventCode": document["properties"]["events"][0]["code"],
        "eventDescription": document["properties"]["events"][0]["description"],
        "magnitudeOfDelay": document["properties"]["magnitudeOfDelay"],
        "city": city,
        "district": district,
    }
    return cleaned_incident


def get_upsert_incident_requests(documents):
    update_requests = []
    for document in documents:
        update_requests.append(
            UpdateOne({"_id": document["_id"]}, {"$set": document}, upsert=True)
        )

    return update_requests


def get_delete_incident_requests(documents):
    delete_requests = []
    for document in documents:
        delete_requests.append(DeleteOne({"_id": document["_id"]}))

    return delete_requests


def run():
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())

    while True:
        raw_incidents = list(TRAFFIC_RAW_COLLECTION.find())
        print(f"Raw length={len(raw_incidents)}")
        cleaned_incidents = pool.map(clean_raw_data, raw_incidents)
        print("\nFinished Cleaning\n")

        # Removing None from list using remove method
        count = 0
        while None in cleaned_incidents:
            count += 1
            cleaned_incidents.remove(None)
        print(f"Clean none: cleaned length={len(cleaned_incidents)}, none={count}\n")

        upsert_incident_cp_requests = get_upsert_incident_requests(raw_incidents)
        upsert_cleaned_incident_requests = get_upsert_incident_requests(
            cleaned_incidents
        )
        delete_raw_incident_requests = get_delete_incident_requests(raw_incidents)

        print("Up raw incidents to checkpoint")
        TRAFFIC_CP_COLLECTION.bulk_write(upsert_incident_cp_requests)
        print("Delete raw incidents in raw traffic")
        TRAFFIC_RAW_COLLECTION.bulk_write(delete_raw_incident_requests)
        print("Up cleaned incidents to clean traffic")
        TRAFFIC_CLEAN_COLLECTION.bulk_write(upsert_cleaned_incident_requests)

        time.sleep(SLEEP_TIME)
        print("Waking up!\n")


if __name__ == "__main__":
    run()
