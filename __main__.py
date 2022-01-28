import pandas as pd
from io import StringIO
from ftpretty import ftpretty
from config import ftp_data_local, ftp_data_aa
import pysftp
import pathlib
import os
from datetime import datetime


def categorise(row):
    if row["quantity_availible"] == 0:
        return "out-of-stock"
    else:
        return "active"


def main():

    f = ftpretty(
        ftp_data_local["host"], ftp_data_local["username"], ftp_data_local["password"]
    )
    bytes = f.get(
        "/Inventory_Update/inventory_available.csv",
    )
    csv = str(bytes, "utf-8")
    data = StringIO(csv)
    df = pd.read_csv(data)
    df.columns = ["sku", "quantity_availible"]
    df["mpn"] = df["sku"]
    df["status"] = df.apply(lambda row: categorise(row), axis=1)
    df = df[["sku", "mpn", "status", "quantity_availible"]]
    filename = f"tmp/Inventory_{datetime.today().strftime('%m%d%Y')}.csv"
    df.to_csv(filename)
    print(df)

    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    sftp = pysftp.Connection(
        host=ftp_data_aa["host"],
        username=ftp_data_aa["username"],
        password=ftp_data_aa["password"],
        cnopts=cnopts,
    )
    file_path = rf"{pathlib.Path().resolve()}\{filename}"
    sftp.cwd("Inbound/")
    sftp.put(file_path)
    sftp.close()
    os.remove(file_path)


if __name__ == "__main__":
    main()
