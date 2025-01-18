from datetime import datetime
import pandas as pd
from google.cloud import bigquery
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
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
                query = f"SELECT * FROM `{full_table_ref}`"
            query_job = client.query(query)
            results = query_job.result()
            dataframe = results.to_dataframe()
            return dataframe
        except Exception as e:
            return pd.DataFrame()

    json_key_path = "/opt/nifi/scripts/linear-theater-442800-s1-26e4d70ab28a.json"
    project_id = "linear-theater-442800-s1"
    dataset_id = "STAGING"
    table_id = "product_raw"
    query = '''SELECT DISTINCT * FROM `linear-theater-442800-s1.STAGING.product_raw` '''

    df_products = run_bigquery_query_to_dataframe(json_key_path, project_id, dataset_id, table_id, query)

    options = webdriver.ChromeOptions()
    options.binary_location = "/usr/bin/google-chrome"
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    data_list = []

    for index, row in df_products.head(5).iterrows():
        product_id = row['Product_ID']
        product_url = row['URL']

        driver.get(product_url)
        time.sleep(5)

        for i in range(5):
            driver.execute_script("window.scrollBy(0, 1000);")
            time.sleep(2)

        try:
            comments = driver.find_elements(By.CLASS_NAME, "review-comment__content")
            users = driver.find_elements(By.CLASS_NAME, "review-comment__user-name")
            dates_on_platform = driver.find_elements(By.CLASS_NAME, "review-comment__user-date")
            titles = driver.find_elements(By.CLASS_NAME, "review-comment__title")
            review_dates = driver.find_elements(By.CLASS_NAME, "review-comment__created-date")
            ratings = driver.find_elements(By.CLASS_NAME, "review-comment__rating-title")

            stars = []
            for rating in ratings:
                try:
                    width_div = rating.find_element(By.CSS_SELECTOR, "div[style*='width']")
                    style = width_div.get_attribute("style")
                    if "width" in style:
                        width_percent = int(style.split("width:")[1].strip().replace("%;", "").replace(";", ""))
                        star_count = width_percent // 20
                        stars.append(star_count)
                except Exception:
                    stars.append(0)

            for i in range(min(len(comments), 10)):
                comment = comments[i].text if i < len(comments) else "N/A"
                user = users[i].text if i < len(users) else "N/A"
                date_on_platform = dates_on_platform[i].text if i < len(dates_on_platform) else "N/A"
                title = titles[i].text if i < len(titles) else "N/A"
                created_date = review_dates[i].text if i < len(review_dates) else "N/A"
                star = stars[i] if i < len(stars) else 0

                data_list.append({
                    'Product_ID': product_id,
                    'User': user,
                    'Time_on_Platform': date_on_platform,
                    'Title': title,
                    'Comment_Date': created_date,
                    'Comment': comment,
                    'Rating': star
                })
        except Exception:
            pass

    driver.quit()

    df_comments = pd.DataFrame(data_list)
    if not df_comments.empty:
        df_comments.to_csv(sys.stdout, index=False, encoding="utf-8-sig")
    else:
        print("No comments found.", file=sys.stderr)


if __name__ == "__main__":
    get_data_and_upload()