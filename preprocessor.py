import json
import requests
import logging
import pandas as pd


# TODO: Data cleaning
class Preprocessor:
    def __init__(self, combine_city_data=False, resample_freq=None):
        self.combine_city_data = combine_city_data
        self.resample_freq = resample_freq

        with open("location_cache.json", "r") as cachefile:
            self.location_cache = json.load(cachefile)

        with open("config.json", "r") as configfile:
            self.api_key = json.load(configfile)['maps_api_key']

        # Saving the potentially changed cache to persistent storage.
        with open("location_cache.json", "w") as cachefile:
            json.dump(self.location_cache, cachefile)

    # Mutating the data in various ways to make the data easier to use later.
    def __mutate_data(self, df):
        self.__simplify_location(df)
        df["timestamp"] = pd.to_datetime(df["timestamp"], infer_datetime_format=True)

    # Uses reverse geocoding to replace the "location", "lat" and "lon" columns with city-country.
    def __simplify_location(self, df):
        location_id = df.at[0, "location"]
        lat = df.at[0, "lat"]
        lng = df.at[0, "lon"]

        if not pd.isnull(lat) and not pd.isnull(lng):
            location = self.__get_city_country(str(location_id), lat, lng)
        else:
            location = ""

        del df["lat"]
        del df["lon"]
        df.loc[:, "location"] = location
        logging.info(f"Simplified {location_id}, {lat}, {lng} to {location}")

    # Return a string with the format "city-country" based on the given latitude and longitude
    def __get_city_country(self, location_id, lat, lng):
        # Checking if the location is cached, if not then retrieve it with reverse geocoding.
        if location_id in self.location_cache:
            location = self.location_cache[location_id]
        else:
            location = self.__reverse_geocode(lat, lng)

            # Saving the newly retrieved location in the cache.
            self.location_cache[location_id] = location

        return location

    def __reverse_geocode(self, lat, lng):
        logging.info(f"Reverse geocoding {lat}, {lng}")
        maps_api_url = "https://maps.googleapis.com/maps/api/geocode/json?"
        result_type = "&result_type=locality&result_type=political"
        key = f"&key={self.api_key}"

        # Making a request to the Google Maps reverse geocoding API.
        api_response = requests.get(f"{maps_api_url}latlng={lat},{lng}{result_type}{key}").json()

        try:
            address_comp = api_response["results"][0]["address_components"]

            # Extracting the city and country name by filtering on the address component type.
            city = list(filter(lambda x: x["types"] == ["locality", "political"], address_comp))[0]["long_name"]
            country = list(filter(lambda x: x["types"] == ["country", "political"], address_comp))[0]["long_name"]

            return f"{city}-{country}"
        except IndexError:
            return ""
