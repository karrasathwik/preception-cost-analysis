import os
import io
import time
import pandas as pd
import requests
from src.S3connection import get_s3_client, upload_file_to_s3
from botocore.exceptions import ClientError
from dotenv import load_dotenv


#load_dotenv(r"C:\Users\sathw\preception-cost-analysis\.env")


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


def save_S3(df,filename):
    bucket_name = os.getenv('S3_BUCKET')
    s3_key = f"bronze/{filename}"
    try:
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        #upload to S3
        s3_client = get_s3_client()
        s3_client.put_object(
            bucket=bucket_name,
            object_name=s3_key,
            body = buffer.getvalue(),
            ContentType = "application/x-parquet"
        )
        print(f"file {filename} uploaded successfully to bucket {bucket_name} as {s3_key}")
    except ClientError as e:
            print(f"An error occurred while uploading the file: {e}")
    except Exception as e:
        print(f"generall error: {e}")

if __name__ == "__main__":
    print("Starting NHS PCA Data Extraction...\n")

    # January + February 2026
    RESOURCE_IDS = [
        "PCA_202601",
        "PCA_202602"
    ]
    #conect to s3 once at start
    s3 = get_s3_client()
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
    save_S3(final_df, "nhs_pca_2026_c1.parquet")

    print("\nNHS PCA extraction completed successfully.")