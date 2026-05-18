import os
import pandas as pd

BASE = r"C:\Users\sathw\preception-cost-analysis"
FILE = "nhs_pca_2026_q1.parquet"
path = os.path.join(BASE, "output", FILE)

# Check what files exist in output directory
output_dir = os.path.join(BASE, "output")

df = pd.read_parquet(path)
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
def save_parquet(df,filename):
    try:
        os.makedirs("sliver_output",exist_ok = True)
        path = os.path.join("sliver_output",filename)
        df.to_parquet(path,index=False)
        print(f"file saved sucesfully at{path}")
    except Exception as e:
        print(f"An error occurred while saving the file: {e}")
sliver_df = silver_transformation(df)
print(sliver_df.head())
save_parquet(sliver_df,"nhs_pca_2026_q1_silver.parquet")
