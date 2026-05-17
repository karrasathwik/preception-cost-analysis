import os
import pandas as pd

BASE = r"C:\Users\sathw\preception-cost-analysis"
FILE = "nhs_pca_2026_q1.parquet"
path = os.path.join(BASE, "output", FILE)

# Check what files exist in output directory
output_dir = os.path.join(BASE, "output")

df = pd.read_parquet(path)
#data quality checks
def data_quality_checks(df):
    for col in df.columns:
        null_cnt = df[col].isnull().sum()
        if null_cnt > 0:
            print(f"Column '{col}' has {null_cnt} null values.")
    if null_cnt == 0:
        print("no null values found in the dataset.")
    for col in df.columns:
        col_cnt = df[col].count()
        if col_cnt != 192000:
            print(f"Column '{col}' has {col_cnt} non-null values, expected 192000.")
    else:
        print("All columns have the expected number of non-null values (192000).")
    #changing the data types of the columns
    df["YEAR_MONTH"] = pd.to_datetime(df["YEAR_MONTH"], format="%Y%m")
    changed_data_type = [
    "BNF_PARAGRAPH_CODE",
    "BNF_SECTION_CODE",
    "BNF_CHAPTER_CODE",
    "PREP_CLASS",
    "PRESCRIBED_PREP_CLASS"]
    for col in changed_data_type:
        if col in df.columns:
            df[col] = df[col].astype(int)
    #adding the dervied columns
    df["COST_PER_ITEM"] = df["NIC"] / df["TOTAL_QUANTITY"]
    #DELETING THE UNNECESSARY COLUMNS
    df = df.drop(columns=["SOURCE_FILE"])
    print("Data quality checks completed successfully.")
    return df
new_df =data_quality_checks(df)
print(new_df.head())

