import os
import time
import pandas as pd
import requests


def get_pca_data(resource_id: str, limit: int = 200000):

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

        # Check API success
        if not data.get("success"):
            print(f"API Error for {resource_id}:")
            print(data.get("error"))
            break

        # Extract records
        records = data["result"]["records"]

        # Stop if no more data
        if not records:
            break

        # Add records
        all_records.extend(records)

        offset += limit

        print(f"{resource_id} -> Fetched {offset} rows...")

        # Avoid overloading API
        time.sleep(0.2)

    # Convert to DataFrame
    return pd.DataFrame(all_records)


def save_csv(df, filename):

    # Create output folder if missing
    os.makedirs("output", exist_ok=True)

    # Full file path
    path = f"output/{filename}"

    # Save CSV
    df.to_csv(path, index=False)

    print(f"\nSaved file -> {path}")


if __name__ == "__main__":

    # January + February 2026
    RESOURCE_IDS = [
        "PCA_202601",
        "PCA_202602"
    ]

    all_dataframes = []

    print("Starting NHS PCA Data Extraction...\n")

    # Loop through all months
    for resource_id in RESOURCE_IDS:

        print(f"Fetching data for {resource_id}...")

        df = get_pca_data(resource_id)

        print(f"{resource_id} Shape: {df.shape}")

        # Add source month column
        df["SOURCE_FILE"] = resource_id

        # Store dataframe
        all_dataframes.append(df)

    # Combine all months
    final_df = pd.concat(all_dataframes, ignore_index=True)

    print("\nFinal Combined Shape:", final_df.shape)

    print("\nPreview:")
    print(final_df.head())

    # Save final dataset
    save_csv(final_df, "pca_jan_feb_2026.csv")

    print("\nNHS PCA extraction completed successfully.")