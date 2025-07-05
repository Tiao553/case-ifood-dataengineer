from config.settings import TAXI_TYPES, MONTHS, BASE_URL

def build_urls():
    urls = []
    for taxi_type in TAXI_TYPES:
        for month in MONTHS:
            filename = f"{taxi_type}_tripdata_{month}.parquet"
            url = f"{BASE_URL}/{filename}"
            urls.append(url)
    return urls
