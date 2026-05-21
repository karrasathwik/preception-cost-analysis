import pandas as pd
import numpy as np
from src.Gold.connections import Connection

class Gold_layer:
    def __init__(self,server,database):
        #set up connections
        self.conn = Connection()
        self.conn.initialize_connection(server,database)
        self.con = self.conn.get_duckdb()
        self.client = self.conn.get_s3_client()
        self.engine = self.conn.get_sql_engine()
    def read_Silver(self,s3_path):
        try:
            df = self.con.execute(
                f"""
                select * from read_parquet('{s3_path}')
                """
            ).df()
            print("Data read successfully from Silver layer.")
            print(df.head())
            return df
        except Exception as e:
            print(f"An error occurred while reading from Silver layer: {e}")
            return None
    def run(self,s3_path):
        return self.read_Silver(s3_path)
    #surrgate key generation
    def add_surrogate_key(self,df:pd.DataFrame,sk_name:str):
        df.insert(0,sk_name,range(1,len(df)+1))
        return df
    def dim_date(self,s3_path:str):
        try:
            df = self.con.execute(
                f"""
                select 
                    distinct year_month
                from read_parquet('{s3_path}')
                 """""
            ).df()
            df = self.add_surrogate_key(df,"date_sk")
            print("Date dimension created successfully.")
            self.load_to_sql(df,"dim_date")
        except Exception as e:
            print(f"dim_date error:{e}")
            return None
    def dim_location(self,s3_path:str):
        try:
            df = self.con.execute(
                f"""
                    SELECT distinct 
                        region_name,
                        region_code,
                        icb_name,
                        icb_code
                    from read_parquet('{s3_path}')
                """
            ).df()
            df = self.add_surrogate_key(df,"location_sk")
            print("location dimension created")
            self.load_to_sql(df,"dim_location")
        except Exception as e:
            print(f"error has occured durring creating dim_location",{e})
            return None
    def dim_drug(self,s3_path:str):
        try:
            df = self.con.execute(
                f"""
                    SELECT DISTINCT
                        bnf_presentation_code,
                        bnf_presentation_name,
                        snomed_code,
                        generic_bnf_equivalent_name,
                        bnf_chemical_substance_code,
                        bnf_chemical_substance,
                        bnf_paragraph_code,
                        bnf_paragraph,
                        bnf_section_code,
                        bnf_section,
                        bnf_chapter_code,
                        bnf_chapter
                    from read_parquet('{s3_path}')
                    

                """
            ).df()
            df = self.add_surrogate_key(df,"drug_sk")
            print("dim_drug created and added surrigated key")
            self.load_to_sql(df,"dim_drugs")
        except Exception as e:
            print(f"error has made during dim_drug:{e}")
        
    def load_to_sql(self,df:pd.DataFrame,table_name:str):
        try:
            df.to_sql(
                name = table_name,
                con = self.engine,
                if_exists = "replace",
                index = False
            )
            print(f"Data loaded successfully to SQL Server table: {table_name}")
        except Exception as e:
            print(f"An error occurred while loading to SQL Server: {e}")
    def run_all(self,s3_path:str):
        print("Starting Gold layer process...")
        self.read_Silver(s3_path)
        #loading dimension tables
        self.dim_date(s3_path)
        print("Gold layer process completed successfully.")
        #loading the data to dim_location
        self.dim_location(s3_path)
        print("Gold layer process completed for dim_location ")
        #loading to dim_drug data
        self.dim_drug(s3_path)
        print("dim_location has sent sql")
        self.conn.close_duckdb()
          
if __name__ == "__main__":
    try:
        gold = Gold_layer(
            server = r"SATHWIKKARRA\SQLEXPRESS",
            database = "NHS_Gold",
            #username = r"SATHWIKKARRA\sathw",
            #password = "Sathwik@123"
        )
        gold.run_all(
            s3_path = "s3://nhs-prescription-project/silver/nhs_pca_2026_cleaned.parquet"
        )
    except Exception as e:
        print(f"An error occurred in the Gold layer process: {e}")