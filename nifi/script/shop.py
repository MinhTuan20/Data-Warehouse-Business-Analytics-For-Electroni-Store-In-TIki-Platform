from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sys

def get_data_and_upload():
    def run_bigquery_query_to_dataframe(json_key_path, project_id, dataset_id, table_id, query):
        """
        Executes a BigQuery SQL query and returns the result as a pandas DataFrame.

        Args:
            json_key_path (str): Path to the JSON key file for authentication.
            project_id (str): Google Cloud project ID.
            dataset_id (str): BigQuery dataset ID.
            table_id (str): BigQuery table ID.
            query (str): SQL query to execute.

        Returns:
            pd.DataFrame: Query results as a pandas DataFrame.
        """
        try:
            client = bigquery.Client.from_service_account_json(json_key_path)
            full_table_ref = f"{project_id}.{dataset_id}.{table_id}"
            if not query:
                query = f"SELECT * FROM `{full_table_ref}` LIMIT 1000"
            query_job = client.query(query)
            results = query_job.result()
            dataframe = results.to_dataframe()
            return dataframe
        except Exception as e:
            return pd.DataFrame()

    json_key_path = "/opt/nifi/scripts/linear-theater-442800-s1-26e4d70ab28a.json"
    project_id = "linear-theater-442800-s1"
    dataset_id = "test_crawl"
    table_id = "test_product_table"
    query = ''' SELECT distinct * FROM `linear-theater-442800-s1.Raw.product_raw` '''

    df_products = run_bigquery_query_to_dataframe(json_key_path=json_key_path, project_id=project_id, dataset_id=dataset_id, table_id=table_id, query=query)

    options = webdriver.ChromeOptions()
    options.binary_location = "/usr/bin/google-chrome"
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    df_shops_list = []

    for index, row in df_products.head(5).iterrows():
        product_id = row['Product_ID']
        product_url = row['URL']

        driver.get(product_url)
        driver.set_window_size(1920, 1080)
        time.sleep(5)

        for _ in range(10):
            driver.execute_script("window.scrollBy(0, 1000);")
            time.sleep(2)

        try:
            clickable_element = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "class-to-trigger-content"))
            )
            clickable_element.click()
            time.sleep(5)
        except Exception:
            pass

        driver.refresh()
        time.sleep(5)

        try:
            sellers = driver.find_element(By.CLASS_NAME, "seller-name")
            seller_links = driver.find_element(By.XPATH, '//span[@class="seller-name"]//a')
            seller_name = sellers.text
            seller_url = seller_links.get_attribute('href')
        except Exception:
            seller_name, seller_url = "N/A", "N/A"

        try:
            review_count_element = driver.find_element(By.CLASS_NAME, "sub-title")
            review_count = review_count_element.text
        except Exception:
            review_count = "N/A"

        df_shops_list.append({
            'Product_ID': product_id,
            'Shop_Name': seller_name,
            'Shop_URL': seller_url,
            'Review_Count': review_count
        })

    df_shops = pd.DataFrame(df_shops_list)
    if not df_shops .empty:
        df_shops.to_csv(sys.stdout, index=False, encoding="utf-8-sig")
    else:
        print("No products fetched.", file=sys.stderr)

if __name__ == "__main__":
    get_data_and_upload()
