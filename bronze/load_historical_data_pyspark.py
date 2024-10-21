from pyspark.shell import spark
from pyspark.sql import SparkSession
from util import *
import argparse


if __name__ == '__main__':

    #Define spark session
    spark = SparkSession.builder.master("local[*]").appName("Historical Load").getOrCreate()

    # pulling data from API

    url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

    # using python request lib, fetching data from given url
    response = fetch_api_data(url)

    print(response)