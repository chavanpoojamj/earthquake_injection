from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, StringType, LongType, StructType, StructField
from util import *
from datetime import datetime
import os
import requests  # Ensure you import requests if you haven't

if __name__ == '__main__':
    # Define spark session
    spark = SparkSession.builder.master("local[*]").appName("Historical Load").getOrCreate()

    # API URLs for fetching monthly and daily earthquake data
    monthly_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"

    # Fetch last month's data using python request lib.
    data_month = fetch_api_data(monthly_url)
    print("Fetched last month's data:", data_month)

    # Base URL for details
    base_url = "https://earthquake.usgs.gov/"
    endpoint_url = "earthquakes/feed/v1.0/detail/"

    # Flatten the data and include the URL for details
    flattened_data = [
        {
            "id": earthquake['id'],
            "mag": float(earthquake['properties']['mag']) if earthquake['properties']['mag'] is not None else None,
            "place": earthquake['properties']['place'],
            "time": earthquake['properties']['time'],
            "status": earthquake['properties']['status'],
            "tsunami": earthquake['properties']['tsunami'],
            "sig": earthquake['properties']['sig'],
            "longitude": earthquake['geometry']['coordinates'][0],
            "latitude": earthquake['geometry']['coordinates'][1],
            "depth": float(earthquake['geometry']['coordinates'][2]) if earthquake['geometry']['coordinates'][
                                                                            2] is not None else None,
            "detail_url": f"{base_url}{endpoint_url}{earthquake['id']}.geojson"  # Construct the URL
        }
        for earthquake in data_month['features']
    ]

    # Define the schema
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("mag", DoubleType(), True),
        StructField("place", StringType(), True),
        StructField("time", LongType(), True),
        StructField("status", StringType(), True),
        StructField("tsunami", IntegerType(), True),
        StructField("sig", IntegerType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("depth", DoubleType(), True),
        StructField("detail_url", StringType(), True)  # New field for the detail URL
    ])

    # Create the Spark DataFrame from the flattened data and schema
    data_month_df = spark.createDataFrame(flattened_data, schema)

    # Show the schema of the DataFrame
    data_month_df.printSchema()

    # Show the first 10 rows of the DataFrame
    data_month_df.show(10, truncate=False)

    # Define the target path
    current_date = datetime.now().strftime("%Y%m%d")
    target_file = "earthquake_bronze_data"  # Name of the target file
    target_path = f"earthquakeanalysis/raw/{current_date}/{target_file}.parquet"

    # Create the target directory if it does not exist
    os.makedirs(os.path.dirname(target_path), exist_ok=True)

    # Save the DataFrame as Parquet
    data_month_df.write.mode("overwrite").parquet(target_path)

    # Show a message indicating where the data has been saved
    print(f"Data saved to: {target_path}")

    # Optionally, you can show the first few rows of the DataFrame
    data_month_df.show(truncate=False)
