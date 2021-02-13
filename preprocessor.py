import json


# TODO: Data cleaning
class Preprocessor:
    def __init__(self, combine_city_data=False, resample_freq=None):
        self.combine_city_data = combine_city_data
        self.resample_freq = resample_freq

        with open("location_cache.json", "r") as cachefile:
            self.location_cache = json.load(cachefile)

        with open("config.json", "r") as configfile:
            self.api_key = json.load(configfile)['maps_api_key']
