# Continue from your existing code
from pyspark.sql import SparkSession
from datetime import datetime
from util import flatten_earthquake_data, get_earthquake_schema, fetch_api_data, \
    fetch_detail_data  # Import the function
import os

if __name__ == '__main__':
    # Define Spark session
    spark = SparkSession.builder.master("local[*]").appName("Historical Load").getOrCreate()

    # API URLs for fetching monthly and daily earthquake data
    monthly_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.geojson"
    daily_url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

    # Fetch last month's data
    data_month = fetch_api_data(monthly_url)

    if data_month:
        print("Successfully fetched last month's earthquake data.")

        # Flatten the monthly data
        flattened_data_month = flatten_earthquake_data(data_month)

        # Create Spark DataFrame for monthly data
        schema = get_earthquake_schema()
        data_month_df = spark.createDataFrame(flattened_data_month, schema)

        # Save the monthly DataFrame as Parquet
        current_date = datetime.now().strftime("%Y%m%d")
        target_path_month = f"earthquakeanalysis/raw/{current_date}/earthquake_bronze_data.parquet"
        os.makedirs(os.path.dirname(target_path_month), exist_ok=True)
        data_month_df.write.mode("overwrite").parquet(target_path_month)
        print(f"Monthly data saved to: {target_path_month}")

        # Fetch daily earthquake data
        data_day = fetch_api_data(daily_url)

        if data_day:
            print("Successfully fetched daily earthquake data.")

            # Flatten the daily data
            flattened_data_day = flatten_earthquake_data(data_day)

            # Create Spark DataFrame for daily data
            data_day_df = spark.createDataFrame(flattened_data_day, schema)

            # Save the daily DataFrame as Parquet
            target_path_day = f"earthquakeanalysis/raw/{current_date}/earthquake_daily_data.parquet"
            os.makedirs(os.path.dirname(target_path_day), exist_ok=True)
            data_day_df.write.mode("overwrite").parquet(target_path_day)
            print(f"Daily data saved to: {target_path_day}")

            # Extract detail URLs and fetch detailed data
            for earthquake in flattened_data_day:
                detail_url = f"https://earthquake.usgs.gov/earthquakes/feed/v1.0/detail/{earthquake['id']}.geojson"
                detail_data = fetch_detail_data(detail_url)  # Use the imported function

                if detail_data:
                    # Flatten the detail data as needed
                    flattened_detail_data = flatten_earthquake_data(detail_data)

                    # Create Spark DataFrame for detail data
                    detail_data_df = spark.createDataFrame(flattened_detail_data, schema)

                    # Save the detail DataFrame as Parquet using the id in the filename
                    target_path_detail = f"earthquakeanalysis/raw/{current_date}/{earthquake['id']}_detail_data.parquet"
                    os.makedirs(os.path.dirname(target_path_detail), exist_ok=True)
                    detail_data_df.write.mode("overwrite").parquet(target_path_detail)
                    print(f"Detail data for {earthquake['id']} saved to: {target_path_detail}")
                else:
                    print(f"Failed to fetch detail data for {earthquake['id']}.")
        else:
            print("Failed to fetch daily earthquake data.")
    else:
        print("Failed to fetch last month's earthquake data.")
