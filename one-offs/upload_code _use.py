import pandas as pd
from modules.db import PostGres, DataLoader
import os

def main():
    d = []
    for file in os.listdir("data/"):
        df = pd.read_csv(f"data/{file}", thousands=",")
        df = df.rename(columns={"Zip Code": "Zip"})
        df = df[['Date', 'Timestamp (CT)', 'Order ID', 'Channel', 'Discount Code',
       'Revenue', 'Campaign Detail', 'Last Touch Channel', 'Response', 'Zip',
       'Customer Type']]
        df["Client"] = file.split(".")[0]
        d.append(df)
    data = pd.concat(d)
    dl = DataLoader(data)
    data = dl.data
    conn_str = f'dbname=test2 user={os.getenv("PGUSER")} password={os.getenv("PGPASSWORD")}' 
    pg = PostGres(conn_str,"codes", data=data)
    pg.createTable()
    pg.insertInto()
if __name__ == "__main__":
    main()
