import os
import pandas as pd
import boto3
import io
from src.S3connection import get_s3_client, upload_file_to_s3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
load_dotenv()

def silver_transformation(df):
    #quality checks
    try:
        nulls = df.isnull().sum()
        if (nulls>0).any():
            print("nulls has been found")
        #fixing data types
        df["YEAR_MONTH"] = pd.to_datetime(df["YEAR_MONTH"], format="%Y%m")
        for col in ["BNF_PARAGRAPH_CODE",
        "BNF_SECTION_CODE",
        "BNF_CHAPTER_CODE",
        "PREP_CLASS",
        "PRESCRIBED_PREP_CLASS"]:
            if col in df.columns:
                df[col] = df[col].astype("int64")
        #adding dervied columns
        df["COST_PER_ITEM"] = (df["NIC"] / df["ITEMS"]).round(4)
        df["PREP_CLASS_LABEL"] = df["PREP_CLASS"].map({1:"Generic",2:"Branded Generic",3:"Branded", 4:"Specially Manufactured"})
        df["HAS_GENERINC_EQUIVALENT"] = (df["GENERIC_BNF_EQUIVALENT_CODE"].notna()&(df["GENERIC_BNF_EQUIVALENT_CODE"] != df["BNF_PRESENTATION_CODE"])).map({True: "Yes", False: "No"})
        #REMOVE uncessary columns
        df = df.drop(columns=["SOURCE_FILE"])
        #changing to lowercase
        df.columns = df.columns.str.lower()
        #title the data frame
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].str.title()
    except Exception as e:
        print(f"An error occurred during transformation: {e}")
    return df

def read_data_parquet(bucket_name, s3_key):
    client = get_s3_client()
    print("connection has established successfully to s3")
    try:
        response = client.get_object(Bucket = bucket_name,Key = s3_key)
        df = pd.read_parquet(io.BytesIO(response["Body"].read()))
        return df
    except ClientError as e:
        print(f"An error occured while reading the file from S3:{e}")
    except Exception as e:
        print(f"An unxepected error occured ",{e})

def save_back_s3(df,filename):
    bucket_name = "nhs-prescription-project"
    s3_key = f"silver/{filename}"
    try:
        buffer = io.BytesIO()
        df.to_parquet(buffer,index = False)
        buffer.seek(0)
        s3_client = get_s3_client()
        s3_client.put_object(
            Bucket= bucket_name,
            Key = s3_key,
            Body = buffer.getvalue(),
            ContentType = "application/x-parquet"
        )
        print(f"file {filename} uploades done successfully to bucket {bucket_name} as {s3_key}")
    except ClientError as e:
        print(f"An error occured during uploading the file:{e}")
    except Exception as e:
        print(f"An unexpted general error has occured during the uploading the file:{e}")
if __name__ == "__main__":
    print("starting transformation process...")
    Bucket_name = "nhs-prescription-project"
    s3_key = "bronze/nhs_pca_2026_raw.parquet"
    read_data = read_data_parquet(Bucket_name, s3_key)
    print("data has sent to reading  the data")
    #send the data to transformation function
    save_back_s3(read_data, "nhs_pca_2026_cleaned.parquet")
    print("\n transformation process completed successfully")