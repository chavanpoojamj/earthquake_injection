from pyspark.shell import spark
from pyspark.sql import SparkSession
from util import *
import argparse


if __name__ == '__main__':

    #Define spark session
    spark = SparkSession.builder.master("local[*]").appName("Historical Load").getOrCreate()

    # pulling data from API
    # API URLs for fetching monthly and daily earthquake data

    monthly_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

    daily_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"


    # Fetch last month's data using python request lib.
    data_month = fetch_api_data(monthly_url)
    print("Fetched last month's data:", data_month)

    # Fetch last day's data using python request lib.
    data_day = fetch_api_data(daily_url)
    print("Fetched last day's data:", data_day)