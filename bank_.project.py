# Code for ETL operations on Country-GDP data
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

# Importing the required libraries

def extract(url, table_attribs):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    table = soup.find('table', {'class': 'wikitable sortable mw-collapsible'})
    bank_data = []
    headers = [header.text.strip() for header in table.find_all('th')]
    rows = table.find_all('tr')[1:]  # Skip the header row
    for row in rows:
        cols = row.find_all('td')
        if len(cols) != 0:
            if '—' not in cols[2].text:  # Corrected typo here
                rank = cols[0].text.strip()
                bank_name = cols[1].text.strip()
                market_cap = cols[2].text.strip()
                # Append the data as a dictionary to the list
                data_dict = {"Rank": rank, "BankName": bank_name, "MarketCap": market_cap}
                bank_data.append(data_dict)
    
    # Create DataFrame from the list of dictionaries
    df = pd.DataFrame(bank_data)
    return df

def transform(df):
    exchange_rate_df = pd.read_csv('/mnt/d/DS Projects/ETL/exchange_rate.csv')
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']

    GDP_list = df["MarketCap"].tolist()
    GDP_list = [float("".join(x.split(','))) for x in GDP_list]
    GDP_list = [np.round(x/1000,2) for x in GDP_list]
    df["MarketCap"] = GDP_list
    df=df.rename(columns = {"MarketCap":"MC_USD_Billion"})
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['MC_USD_Billion']]
    print(f"output of df['MC_EUR_Billion'][4]:",df['MC_USD_Billion'][5])
    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)
    
def load_to_db(df, sql_connection, table_name):
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message): 
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')


url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = "wikitable sortable mw-collapsible" #we can start here analysiing the whole HTML segement
csv_path="Largest_banks_data.csv"
db_name= "Banks.db"
table_name="Largest_banks"
log_file="code_log.txt"


log_progress('Preliminaries complete. Initiating ETL process')
df = extract(url, table_attribs)
log_progress('Data extraction complete. Initiating Transformation process')
df = transform(df)
log_progress('Data transformation complete. Initiating loading process')
load_to_csv(df, csv_path)
log_progress('Data saved to CSV file')
sql_connection = sqlite3.connect('World_Economies.db')
log_progress('SQL Connection initiated.')
load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table. Running the query')
query1 = f"SELECT * FROM {table_name}"
query2 = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
query3 = f"SELECT BankName from Largest_banks LIMIT 5"
log_progress('Data loaded to Database as table. Running the query 1')

run_query(query1, sql_connection)
log_progress('Data loaded to Database as table. Running the query 2')

run_query(query2, sql_connection)
log_progress('Data loaded to Database as table. Running the query 3')

run_query(query3, sql_connection)

log_progress('Process Complete.')
sql_connection.close()
