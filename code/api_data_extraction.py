import os
import time
import pandas as pd
import requests
import os

def get_pca_data(resource_id: str, limit: int = 100000):

    url = "https://opendata.nhsbsa.net/api/3/action/datastore_search"

    offset = 0
    all_records = []

    while True:

        params = {
            "resource_id": resource_id,
            "limit": limit,
            "offset": offset
        }

        response = requests.get(url, params=params)
        data = response.json()

        # check API success
        if not data.get("success"):
            print("API Error:", data.get("error"))
            break

        records = data["result"]["records"]

        # stop when no more data
        if not records:
            break

        all_records.extend(records)

        offset += limit

        print(f"Fetched {offset} rows...")

        time.sleep(0.2)  # avoid overloading API

    return pd.DataFrame(all_records)

def save_csv(df, filename):

    # create folder if it doesn't exist
    os.makedirs("output", exist_ok=True)

    # full save path
    path = f"output/{filename}"

    # save csv
    df.to_csv(path, index=False)

    print(f"Saved: {path}")
if __name__ == "__main__":

    RESOURCE_ID = "PCA_202602"

    print("Starting NHS PCA data extraction...")

    df = get_pca_data(RESOURCE_ID)

    print("Shape:", df.shape)
    print(df.head())

    save_csv(df, "pca_202602.csv")