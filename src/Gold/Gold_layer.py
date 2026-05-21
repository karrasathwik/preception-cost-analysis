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
            return df
        except Exception as e:
            print(f"dim_date error:{e}")
            return None
    def dim_location(self,s3_path:str):
        try:
            df = self.con.execute(
                f"""
                    SELECT distinct 
                        region_code,
                        region_name,
                        icb_name,
                        icb_code
                    from read_parquet('{s3_path}')
                """
            ).df()
            df = self.add_surrogate_key(df,"location_sk")
            print("location dimension created")
            self.load_to_sql(df,"dim_location")
            return df
        except Exception as e:
            print(f"error has occured durring creating dim_location:{e}")
            return None
    def dim_drug(self,s3_path:str):
        try:
            df = self.con.execute(
                f"""
                    SELECT DISTINCT
                        bnf_presentation_code,
                        bnf_presentation_name,
                        generic_bnf_equivalent_name,
                        bnf_chemical_substance_code,
                        bnf_chemical_substance,
                        bnf_paragraph_code,
                        bnf_paragraph,
                        bnf_section_code,
                        bnf_section,
                        bnf_chapter_code,
                        bnf_chapter,
                        has_generinc_equivalent,
                        prescribed_prep_class,
                        pharmacy_advanced_service,
                        prep_class_label
                    from read_parquet('{s3_path}')
                    

                """
            ).df()
            df = self.add_surrogate_key(df,"drug_sk")
            print("dim_drug created and added surrigated key")
            self.load_to_sql(df,"dim_drugs")
            return df
        except Exception as e:
            print(f"error has made during dim_drug:{e}")
    def dim_supplier(self,s3_path):
        try:
            df = self.con.execute(
                f"""
                    SELECT DISTINCT
                        supplier_name,
                        unit_of_measure,
                        dispenser_account_type
                        
                    FROM read_parquet('{s3_path}')
                """
            ).df()
            df = self.add_surrogate_key(df,"supplier_sk")
            print("supplier_Sk is added")
            self.load_to_sql(df,"dim_supplier")
            return df
        except Exception as e:
            print("you have a error at",{e})

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

    def dim_fact(self,s3_path,dim_date_df,dim_location_df,dim_drug_df,dim_supplier_df,):
        try:
            print("dim_location columns :", dim_location_df.columns.tolist())
            print("dim_drug columns:",dim_drug_df.columns.tolist())
            fact_df = self.con.execute(
                f"""
                    SELECT
                        year_month,
                        region_code,
                        icb_code,
                        bnf_presentation_code, 
                        items,
                        total_quantity,
                        supplier_name,
                        nic,
                        cost_per_item
                    FROM read_parquet('{s3_path}')
                """).df()
            print(f"step 1-rows loaded from sliver:{len(fact_df)}")
            fact_df = fact_df.merge(
                dim_date_df[["date_sk","year_month"]],on="year_month",how="left"
            )
            print(f"STEP 2 — After date merge: {len(fact_df)}")
            #merger location
            fact_df = fact_df.merge(dim_location_df[["location_sk","region_code"]],
                                    on ="region_code",
                                    how ="left"    
            )
            print(f"After the dim_location_merge:{len(fact_df)}")
            #merge dim_drugs
            fact_df = fact_df.merge(
                 dim_drug_df[["drug_sk","bnf_presentation_code"]],
                 on = "bnf_presentation_code",
                 how ="left"
            ) 
            print(f"STEP 4 — After drug merge: {len(fact_df)}")
            fact_df = fact_df.merge(
                dim_supplier_df[["supplier_sk", "supplier_name"]],
                on="supplier_name",
                how="left"
            )
            print(f"STEP 5 — After supplier merge: {len(fact_df)}")
            fact_df.drop(columns=[
                "year_month",
                "region_code",
                "icb_code",
                "bnf_presentation_code",
                "supplier_name"
            ])
            df = self.add_surrogate_key(fact_df,"fact_sk")
            print(f"\nFinal fact table shape: {fact_df.shape}")
            print(f"final columns:{fact_df.columns.to_list}")
            self.load_to_sql(fact_df,"dim_fact")
            print("done with fact")
            return fact_df
        except Exception as e:
            print(f"you have error in fact,{e}")
            
    def run_all(self,s3_path:str):
        print("Starting Gold layer process...")
        self.read_Silver(s3_path)
        #loading dimension tables
        dim_date_df     = self.dim_date(s3_path)
        dim_location_df = self.dim_location(s3_path)
        dim_drug_df     = self.dim_drug(s3_path)
        dim_supplier_df = self.dim_supplier(s3_path)
        self.dim_fact(
            s3_path,
            dim_date_df,
            dim_location_df,
            dim_drug_df,
            dim_supplier_df
        )
        print("Gold layer complete")
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