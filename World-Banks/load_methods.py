import logging
import pandas as pd
import requests
import sqlite3
from bs4 import BeautifulSoup
import plotly.express as px
import plotly.graph_objects as go


def load_to_csv(output_path, dataframe):
    logging.info(f"Loading into {output_path}...")

    dataframe.to_csv(output_path, index=False)

    logging.info(f"Load into {output_path} did Success!...")


def load_to_sql(db, dataframe):
    logging.info(f"Loading into {db}...")

    TABLE_NAME = "Largest_banks"
    connection = sqlite3.connect(db)
    dataframe.to_sql(TABLE_NAME, con=connection, if_exists="replace", index=False)

    connection.close()

    logging.info(f"Load into {db} did Success!...")


def verify_sql_data(db):
    connection = sqlite3.connect(db)
    cur = connection.cursor()
    cur.execute("select * from Largest_banks")

    rows = cur.fetchall()

    connection.close()

    for row in rows:
        print(row)
