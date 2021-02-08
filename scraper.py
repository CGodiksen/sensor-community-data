class Scraper:
    def __init__(self, start=None, end=None, sensor_types=None, sensor_ids=None, locations=None, measurements=None):
        self.url = "https://archive.sensor.community/"
        self.start = start
        self.end = end
        self.sensor_types = sensor_types
        self.sensor_ids = sensor_ids
        self.locations = locations
        self.measurements = measurements

    # Start the scraper with the given settings.
    def start(self):
        pass
        # TODO: Based on the given settings a json config file should be made.
        # TODO: Find all the urls of the dates that are going to be scraped from
        # TODO: Find a list of the files from each date that should be downloaded (based on sensor list, avoid indoor)
        # TODO: If the used config matches an existing config file then don't downloaded already downloaded files.
        # TODO: For each file, download it, process it and remove it.
        # TODO: Processing should include removing extra rows (based on measurements list and removing empty rows)
        # TODO: Location, lat and lon should be turned into a city based on reverse geocoding (maps API)
        # TODO: To avoid repeated API usage a location file should be kept that caches locations.
        # TODO: When the file is processed the data should be written to a file.
