
import logging
import pandas as pd
import requests
import sqlite3
from bs4 import BeautifulSoup
import plotly.express as px
import plotly.graph_objects as go


def extract_tables_from_web(url):
  logging.info(f"Requesting From {url}...")
  res = requests.get(url)
  soup = BeautifulSoup(res.content, 'html.parser')

  logging.info(f"Sucessfully Got and Parsed")

  # Find All Tables
  tables = soup.find_all('table')

  return str(tables[0]) # Only return the first table, which is by market capitalization. As string for pandas.

def extract_exchange_rates_from_csv(file):
  logging.info(f"Reading From CSV {file}...")
  df = pd.read_csv(file)
  logging.info(f"Reading Success From CSV {file}...")
  return df
