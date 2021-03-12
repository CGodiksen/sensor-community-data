# Sensor Community Scraper
Tool for scraping and processing data from the sensor community archive: https://archive.sensor.community/

## Setup
To use the preprocessor to reverse geocode locations that are not in the cache, you need to use the Google Maps API. This can be done by creating the file ```config.json``` in the project root and adding the following to the file:

```
{
  "maps_api_key": "your_api_key"
}
```
For information on how to get an API key, go to https://developers.google.com/maps/documentation/geocoding/get-api-key.

## Design
The data collection tool follows a modular design where the scraper and preprocessor can be used separately. For efficiency reasons, the two can also be combined by passing a preprocessor object to the scraper, allowing data to be piped directly without intermediary storage. The settings used to configure the scraper and preprocessor are described in detail in their respective class docstrings.

### Example
```python
preprocessor = Preprocessor(save_path, combine_city_data=True, resample_freq="60T", add_lockdown_info=True)

scraper = Scraper(["P1", "P2"], start_date=date(2017, 1, 1), end_date=date(2021, 1, 1), sensor_types=["sds011"], 
                  preprocessor=preprocessor)
scraper.start()
```
The above example will scrape all data between 01/01/2017 and 01/01/2021 from the SDS011 sensor, keeping only the data measurements "P1" and "P2". When the data is scraped from the archive it is sent along to the given preprocessor which in this example combines all data within a single city and resamples it so there is a single data point per hour. Furthermore, the preprocessor also adds an additional column specifying whether or not the specific data point was collected during a COVID-19 lockdown as given by the Oxford government response tracker (https://www.bsg.ox.ac.uk/research/research-projects/covid-19-government-response-tracker).
