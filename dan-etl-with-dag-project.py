## Converting raw ETL to automated ETL with Airflow and SQL Alchemey 
## Project: Daniel Wondyifraw

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import sqlalchemy


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'dan-etl-with-dag-project',
    default_args=default_args,
    description='ETL DAG for Bank data scrapping and analytics using wikepeadea and database ',
    schedule_interval=timedelta(days=1),
)

def crawl_web():
    logger.info('Starting web crawl node')
    url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
    response = requests.get(url)
    return response.text

def extract_data(url,table_attribs,ti):
    logger.info('Starting web extract node task')
    html_content = ti.xcom_pull(task_ids='crawl_web')
    soup = BeautifulSoup(html_content, 'html.parser')
    table = soup.find('table', {'class': table_attribs})
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
    data = pd.DataFrame(bank_data)
    logger.info('data extracted succesfully')
    return data

def transform_data(ti):
    logger.info('transforming data now')
    data = ti.xcom_pull(task_ids='extract_data')
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
    print(f"output of df['MC_EUR_Billion'][4]:",df['MC_USD_Billion'][5])git
    transformed_data = df
    return transformed_data


def load_data(ti):
    logger.info('loading data now')

    data = ti.xcom_pull(task_ids='transform_data')
    data.to_csv(csv_path)
    engine = sqlalchemy.create_engine('mysql+pymysql://user:password@host/dbname')  # Replace with actual DB connection
    with engine.connect() as connection:
        connection.execute(f"INSERT INTO table_name (title) VALUES ('{data['title']}')")  # Replace with actual loading logic

def run_query(ti):
    logger.info('querying data now')
    result = []
    connection = ti.xcom_pull(task_ids='load_data')
    query = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks" 
    with engine.connect() as connection:
        result = connection.execute(query)  # query
    logger.info(f"results of query is:{result}")

# Define the tasks
crawl_task = PythonOperator(
    task_id='crawl_web',
    python_callable=crawl_web,
    dag=dag,
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)


run_task = PythonOperator(
    task_id='run_query',
    python_callable=run_query,
    dag=dag,
)

# Set task dependencies
crawl_task >> extract_task >> transform_task >> load_task >> run_task
