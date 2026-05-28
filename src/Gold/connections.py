import duckdb
import pandas as pd
import boto3
import io
from botocore.exceptions import ClientError
from sqlalchemy import create_engine
from src.S3connection import get_s3_client
from dotenv import load_dotenv
import os

load_dotenv()

class Connection:
    def __init__(self):
        self.con = None
        self.client = None
        self.engine = None
    def create_connection(self):
        try:
            self.con = duckdb.connect()
            print("Connection to DuckDB established")
            self.con.execute("Install 'httpfs';")
            self.con.execute("Load 'httpfs';")
            self.con.execute("SET s3_region='us-east-1';")
            self.con.execute(f"SET s3_access_key_id='AKIARO6E62INFWBTL7TP';")
            self.con.execute(f"SET s3_secret_access_key='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';")
            print("DuckDB is ready to use.")
        except Exception as e:
            print(f"An error occured while creatiing Duckdb")
    def connect_to_s3(self):
        try:
            self.client = get_s3_client()
            print("Connection to s3 has been established")
        except Exception as e:
            print(f"An error ocured during connection to s3")
    def connect_to_sql(self,server:str,database:str):
        try:
            self.engine = create_engine(
            f"mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
        )
            print("connection to sql server has been done")
        except Exception as e:
            print(f"An error occured during sql connection",{e})

    def get_duckdb(self):
        return self.con
    def get_s3_client(self):
        return self.client
    def get_sql_engine(self):
        return self.engine
#close connections
    def close_duckdb(self):
        if self.con:
            self.con.close()
            print("DuckDB connection closed.")
        if self.engine:
            self.engine.dispose()
            print("SQL engine connection closed.")
#initialiaze connections
    def initialize_connection(self,server:str,database:str):
        self.create_connection()
        self.connect_to_s3()
        self.connect_to_sql(server,database)
        if self.con and self.client and self.engine:
            print("All connections are established successfully.")
        else:
            print("Failed to establish one or more connections.")
            print("Please check your connection details and try again.")
if __name__ == "__main__":
    conn = Connection()
    conn.initialize_connection(
        server = r"SATHWIKKARRA\SQLEXPRESS",
        database = "NHS_Gold",
       # username = r"SATHWIKKARRA\sathw",
        #password = "Sathwik@123"
        
    )