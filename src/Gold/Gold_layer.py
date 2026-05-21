import pandas as pd
import numpy as np
from src.Gold.connections import Connection


class Gold_layer:
    def __init__(self, server, database):
        self.conn = Connection()
        self.conn.initialize_connection(server, database)
        self.con = self.conn.get_duckdb()
        self.client = self.conn.get_s3_client()
        self.engine = self.conn.get_sql_engine()

    # ---------------- READ ----------------
    def read_Silver(self, s3_path):
        df = self.con.execute(
            f"SELECT * FROM read_parquet('{s3_path}')"
        ).df()
        print("Silver data loaded:", len(df))
        return df

    # ---------------- SURROGATE KEY ----------------
    def add_surrogate_key(self, df, sk_name):
        df = df.copy()
        df.insert(0, sk_name, range(1, len(df) + 1))
        return df

    # ---------------- DATE DIM ----------------
    def dim_date(self, s3_path):
        df = self.con.execute(
            f"""
            SELECT DISTINCT year_month
            FROM read_parquet('{s3_path}')
            """
        ).df()

        df = df.drop_duplicates(subset=["year_month"])
        df = self.add_surrogate_key(df, "date_sk")

        self.load_to_sql(df, "dim_date")
        return df

    # ---------------- LOCATION DIM ----------------
    def dim_location(self, s3_path):
        df = self.con.execute(
            f"""
            SELECT DISTINCT
                region_code,
                region_name,
                icb_code,
                icb_name
            FROM read_parquet('{s3_path}')
            """
        ).df()

        # IMPORTANT: enforce uniqueness
        df = df.drop_duplicates(subset=["region_code", "icb_code"])

        df = self.add_surrogate_key(df, "location_sk")

        self.load_to_sql(df, "dim_location")
        return df

    # ---------------- DRUG DIM ----------------
    def dim_drug(self, s3_path):
        df = self.con.execute(
            f"""
            SELECT DISTINCT
                bnf_presentation_code,
                bnf_presentation_name,
                generic_bnf_equivalent_name,
                bnf_chemical_substance_code,
                bnf_chemical_substance,
                bnf_paragraph_code,
                bnf_section_code,
                bnf_chapter_code,
                has_generinc_equivalent,
                prescribed_prep_class,
                prep_class_label
            FROM read_parquet('{s3_path}')
            """
        ).df()

        df = df.drop_duplicates(subset=["bnf_presentation_code"])
        df = self.add_surrogate_key(df, "drug_sk")

        self.load_to_sql(df, "dim_drugs")
        return df

    # ---------------- SUPPLIER DIM ----------------
    def dim_supplier(self, s3_path):
        df = self.con.execute(
            f"""
            SELECT DISTINCT
                supplier_name,
                unit_of_measure,
                dispenser_account_type
            FROM read_parquet('{s3_path}')
            """
        ).df()

        # IMPORTANT FIX: ensure 1 row per supplier
        df = df.drop_duplicates(subset=["supplier_name"])

        df = self.add_surrogate_key(df, "supplier_sk")

        self.load_to_sql(df, "dim_supplier")
        return df

    # ---------------- LOAD ----------------
    def load_to_sql(self, df, table_name):
        df.to_sql(
            name=table_name,
            con=self.engine,
            if_exists="replace",
            index=False
        )
        print(f"Loaded {table_name}")

    # ---------------- FACT TABLE ----------------
    def dim_fact(self, s3_path, dim_date_df, dim_location_df, dim_drug_df, dim_supplier_df):

        fact_df = self.con.execute(
            f"""
            SELECT
                year_month,
                region_code,
                icb_code,
                bnf_presentation_code,
                supplier_name,
                items,
                total_quantity,
                nic,
                cost_per_item
            FROM read_parquet('{s3_path}')
            """
        ).df()

        print("Initial fact rows:", len(fact_df))

        # DATE
        fact_df = fact_df.merge(
            dim_date_df[["date_sk", "year_month"]],
            on="year_month",
            how="left"
        )

        # LOCATION 
        fact_df = fact_df.merge(
            dim_location_df[["location_sk", "region_code", "icb_code"]],
            on=["region_code", "icb_code"],
            how="left"
        )

        # DRUG
        fact_df = fact_df.merge(
            dim_drug_df[["drug_sk", "bnf_presentation_code"]],
            on="bnf_presentation_code",
            how="left"
        )

        # SUPPLIER
        fact_df = fact_df.merge(
            dim_supplier_df[["supplier_sk", "supplier_name"]],
            on="supplier_name",
            how="left"
        )
        print(fact_df)
        # fact
        fact_df = fact_df.drop(columns=[
            "year_month",
            "region_code",
            "icb_code",
            "bnf_presentation_code",
            "supplier_name"
        ])

        # surrogate key
        fact_df = self.add_surrogate_key(fact_df, "fact_sk")

        print("Final fact rows:", len(fact_df))

        # sanity check
        print("Duplicate rows:", fact_df.duplicated().sum())

        self.load_to_sql(fact_df, "fact_table")
        print(fact_df)
        return fact_df

    # ---------------- RUN ----------------
    def run_all(self, s3_path):
        print("Starting Gold layer...")

        dim_date_df = self.dim_date(s3_path)
        dim_location_df = self.dim_location(s3_path)
        dim_drug_df = self.dim_drug(s3_path)
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


# ---------------- MAIN ----------------
if __name__ == "__main__":
    gold = Gold_layer(
        server=r"SATHWIKKARRA\SQLEXPRESS",
        database="NHS_Gold"
    )

    gold.run_all(
        s3_path="s3://nhs-prescription-project/silver/nhs_pca_2026_cleaned.parquet"
    )