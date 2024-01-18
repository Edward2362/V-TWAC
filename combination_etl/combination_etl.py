import pymongo
import pandas
import time
import os

SLEEP_TIME = int(os.environ.get("SLEEP_TIME", 21600))
CLIENT = pymongo.MongoClient(os.environ.get("MONGO_COMPASS"))

CLEAN_DB = CLIENT.clean
TRAFFIC_CLEAN_COLLECTION = CLEAN_DB.traffic
TRAFFIC_WEATHER_COLLECTION = CLEAN_DB.weather

COMBINATION_DB = CLIENT.combination
TRAFFIC_WEATHER_COMB_COLLECTION = COMBINATION_DB.traffic_weather_combination


def split_incidents(documents):
    rows = []
    for document in documents:
        start = pandas.to_datetime(document["startTime"])
        end = pandas.to_datetime(document["endTime"])
        time_intervals = pandas.date_range(
            start=start.floor("H"), end=end.floor("H"), freq="H"
        )

        for time in time_intervals:
            new_row = document.copy()
            new_row["dt"] = time
            rows.append(new_row)

    return rows


def fill_timestamp(df):
    # Converting time to date time object
    df["date"] = pandas.to_datetime(df["dt"]).dt.date
    df["time"] = pandas.to_datetime(df["dt"]).dt.time
    districts = df["district"].unique()
    dates = df["date"].unique()
    dis_city_map = df.set_index("district")["city"].to_dict()
    new_rows = []
    for district in districts:
        for date in dates:
            distict_date_df = df[(df["district"] == district) & (df["date"] == date)]
            datetimes = list(distict_date_df["dt"].unique())
            time_intervals = list(pandas.date_range(start=date, periods=24, freq="H"))
            for time_interval in time_intervals:
                if time_interval not in datetimes:
                    new_row = {
                        "district": district,
                        "dt": time_interval,
                        "mag1": 0,
                        "mag2": 0,
                        "mag3": 0,
                        "city": dis_city_map.get(district),
                    }
                    new_rows.append(new_row)

    df = pandas.concat([df, pandas.DataFrame(new_rows)], ignore_index=True).drop(
        columns=["date", "time"]
    )

    return df


def run():
    while True:
        print("\nLoad traffic and weather data")
        # Load data from MongoDB to dictionaries
        cleaned_incidents = list(TRAFFIC_CLEAN_COLLECTION.find())
        cleaned_weather = list(TRAFFIC_WEATHER_COLLECTION.find())

        # Split incidents into multiple incidents depends on timestamps between start time and end time
        expanded_cleaned_incidents = split_incidents(cleaned_incidents)
        incidents_df = pandas.DataFrame(expanded_cleaned_incidents).drop(
            columns=["startTime", "endTime"]
        )

        print("Creating incident and weather data frames")
        # Remove timezone of timestamp column
        incidents_df["dt"] = pandas.to_datetime(incidents_df["dt"]).dt.tz_localize(None)
        weather_df = pandas.DataFrame(cleaned_weather)

        print("Grouping incident data by district, timestamp, and magniture of delay")
        # Group incident data frame by district, timestamp, and magniture of delay
        aggregated_df = (
            incidents_df.groupby(["district", "dt", "magnitudeOfDelay"])
            .size()
            .unstack(fill_value=0)
            .reset_index()
        )

        print("Map district to city")
        # Map district to city
        dis_city_map = incidents_df.set_index("district")["city"].to_dict()
        aggregated_df["city"] = aggregated_df["district"].apply(
            lambda row: dis_city_map.get(row)
        )

        # Rename columns
        aggregated_df.columns = ["district", "dt", "mag1", "mag2", "mag3", "city"]

        print("Filling missing timestamp")
        # Generate records of districts that do not have any records
        aggregated_df = fill_timestamp(aggregated_df)

        print("Merging with weather data")
        # Merge aggregated data frame with weather data frame
        aggregated_df = pandas.merge(
            aggregated_df,
            weather_df,
            left_on=["city", "dt"],
            right_on=["location", "ts"],
        )

        print("Up data to combination collection")
        # Drop existed data in collection
        TRAFFIC_WEATHER_COMB_COLLECTION.drop()
        # Drop _id column so that Mongo can generate new _id
        dict_comb = aggregated_df.drop(columns=["_id"]).to_dict(orient="records")
        TRAFFIC_WEATHER_COMB_COLLECTION.insert_many(dict_comb)
        print(f"Uploaded {len(dict_comb)} data")

        time.sleep(SLEEP_TIME)
        print("Waking up!\n")


if __name__ == "__main__":
    run()
