import logging
import pandas as pd
import requests
import sqlite3
from bs4 import BeautifulSoup
import plotly.express as px
import plotly.graph_objects as go

def transform_data(table_str, exchange_rates_df):
  logging.info(f"Transforming Data...")
  df = pd.read_html(table_str)[0]
  df = df.drop(columns=[df.columns[0]])
  df.columns = ["Name", "MC_USD_Billion"]

  for row in exchange_rates_df.itertuples():
      df[f"MC_{row.Currency}_Billion"] = (df["MC_USD_Billion"] * row.Rate).round(2)

  logging.info(f"Transforming Data Ran Succesfully...")

  return df