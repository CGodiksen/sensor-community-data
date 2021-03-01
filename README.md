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
