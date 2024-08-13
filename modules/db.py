import psycopg
import datetime as dt
import pandas as pd
import numpy as np


class PostGres:
    def __init__(self, conn_str: str, table_name: str = None ,data: pd.DataFrame = None) -> None:
        self.connection_string = conn_str
        self.data = data
        self.name = table_name 
        if type(self.data) == pd.DataFrame:
            self.field_names = data.columns.tolist()
        self.representative_row = {}
        self.type_to_type = {str: "VARCHAR(255)", int: "INT", float: "FLOAT", bool: "BOOLEAN",
                             pd.Timestamp: "TIMESTAMPTZ", dt.date: "DATE", dt: "TIMESTAMP", dt.datetime: "TIMESTAMP",
                             np.float64: "FLOAT"}

    def repRow(self) -> None:
        if type(self.data) == pd.DataFrame:
            for c in self.field_names:
                t = self.data[c].dropna()
                if not t.empty:
                    self.representative_row[c] = t.iloc[0]
                else:
                    self.representative_row[c] = ""
        else:
            print("No data provided")
        return self

    def fieldTypes(self) -> str:
        self.repRow()
        fields = "".join([f"{c.replace(" ", "_").replace("(", "").replace(")", "")}  {self.type_to_type[type(self.representative_row[c])]}, " for c in self.field_names]).rstrip(", ")
        formatted = f"({fields})"
        return formatted
    
    def tupValues(self) -> list:
        return [tuple(r[c] for c in self.field_names) for _, r in self.data.iterrows()]

    def createQuery(self) -> str:
        return f"CREATE TABLE IF NOT EXISTS {self.name} {self.fieldTypes()};"

    def insertQuery(self) -> str:
        fields = ", ".join([c.replace(" ", "_").replace("(", "").replace(")", "") for c in self.field_names]).rstrip(", ")
        placeholders = ", ".join(["%s" for _ in self.field_names]).rstrip(", ")
        t = f"INSERT INTO {self.name} ({fields}) VALUES ({placeholders});"
        return t

    def createTable(self) -> None:
        with psycopg.connect(self.connection_string) as conn:
            with conn.cursor() as curr:
                curr.execute(self.createQuery())
        return self
    
    def deleteTable(self):
        with psycopg.connect(self.connection_string) as conn:
            with conn.cursor() as curr:
                curr.execute(f"DROP TABLE IF EXISTS {self.name} RESTRICT;")
        return self
    
    def insertInto(self) -> None:
        with psycopg.connect(self.connection_string) as conn:
            with conn.cursor() as curr:
                curr.executemany(self.insertQuery(), self.tupValues())
        return self

class DataLoader:
    def __init__(self, data: pd.DataFrame) -> None:
        self.data = data
        self.fields = data.columns.tolist()

    def cleanNums(self) -> None:
        pass


