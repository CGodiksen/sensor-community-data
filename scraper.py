class Scraper:
    def __init__(self, start=None, end=None, sensors=None, locations=None, measurements=None):
        self.url = "https://archive.sensor.community/"
        self.start = start
        self.end = end
        self.sensors = sensors
        self.locations = locations
        self.measurements = measurements

    # Start the scraper with the given settings.
    def start(self):
        pass
