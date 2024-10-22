import requests
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType


def fetch_api_data(url):
    """
    This method will fetch data from API using requests library.
    :param url: API URL
    :return: JSON object
    """
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad responses
    return response.json()


def fetch_detail_data(detail_url):
    """
    Fetch detailed earthquake data from the USGS API using the detail URL.
    :param detail_url: URL for the detailed earthquake data.
    :return: JSON object containing detailed earthquake data.
    """
    try:
        response = requests.get(detail_url)
        response.raise_for_status()  # Raise an error for bad responses
        return response.json()  # Get the JSON response
    except requests.exceptions.RequestException as e:
        print(f"Error fetching detail data from {detail_url}: {e}")
        return None  # Handle the error as needed


def flatten_earthquake_data(data):
    """
    Flatten earthquake data from GeoJSON format into a list of dictionaries.

    Args:
        data (dict): Earthquake data in GeoJSON format from the USGS API.

    Returns:
        list[dict]: Flattened earthquake data with keys for 'id', 'mag', 'place', 'time',
                    'status', 'tsunami', 'sig', 'longitude', 'latitude', and 'depth'.
    """
    if 'features' in data:
        # Flattening data from summary response (monthly/daily)
        return [
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
                "depth": float(earthquake['geometry']['coordinates'][2]) if earthquake['geometry']['coordinates'][2] is not None else None
            }
            for earthquake in data['features']
        ]
    else:
        # Flattening detailed earthquake data response
        return [
            {
                "id": data['id'],
                "mag": float(data['properties']['mag']) if data['properties']['mag'] is not None else None,
                "place": data['properties']['place'],
                "time": data['properties']['time'],
                "status": data['properties']['status'],
                "tsunami": data['properties']['tsunami'],
                "sig": data['properties']['sig'],
                "longitude": data['geometry']['coordinates'][0],
                "latitude": data['geometry']['coordinates'][1],
                "depth": float(data['geometry']['coordinates'][2]) if data['geometry']['coordinates'][2] is not None else None
            }
        ]  # Return as a list for consistency


def get_earthquake_schema():
    """
    Define and return the schema for earthquake data.

    Returns:
        StructType: Schema for the earthquake DataFrame.
    """
    return StructType([
        StructField("id", StringType(), True),
        StructField("mag", DoubleType(), True),
        StructField("place", StringType(), True),
        StructField("time", LongType(), True),
        StructField("status", StringType(), True),
        StructField("tsunami", IntegerType(), True),
        StructField("sig", IntegerType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("depth", DoubleType(), True)
    ])
