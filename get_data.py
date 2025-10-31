import os
import requests 
import pyarrow.parquet as pq

#create data folder
data_folder = "nyc_tripdata"
os.makedirs(data_folder, exist_ok=True)

def extract_data(year):
    datasets = ["yellow", "green"]
    months = range(1, 13)
    
    #loop each month in each dataset
    for dataset in datasets:
        for month in months:
            month_number = f"{month:02d}"
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset}_tripdata_{year}-{month_number}.parquet"
            filename = url.split("/")[-1]
            local_path = os.path.join(data_folder, filename)

            try:
                response = requests.get(url, stream=True)
                response.raise_for_status()               

                with open(local_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
            
            except Exception as e:
                print(f"error for {url}: {e}")

    print("files downloaded successfully")

extract_data(year=2024)
