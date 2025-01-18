from datetime import datetime, timedelta
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

# Hàm lấy dữ liệu từ BigQuery và xử lý
def get_data_and_upload():
    def run_bigquery_query_to_dataframe(json_key_path, project_id, dataset_id, table_id, query):
        """
        Executes a BigQuery SQL query and returns the result as a pandas DataFrame.
        """
        try:
            # Initialize BigQuery client
            client = bigquery.Client.from_service_account_json(json_key_path)

            # Construct full table reference if needed
            full_table_ref = f"{project_id}.{dataset_id}.{table_id}"

            # If no specific query provided, default to SELECT all from the table
            if not query:
                query = f"SELECT * FROM `{full_table_ref}` LIMIT 1000"

            # Execute query
            query_job = client.query(query)
            results = query_job.result()

            # Convert to pandas DataFrame
            dataframe = results.to_dataframe()
            return dataframe

        except Exception as e:
            print(f"Error while executing BigQuery query: {e}")
            return pd.DataFrame()
    
    json_key_path = "/opt/nifi/scripts/linear-theater-442800-s1-26e4d70ab28a.json"
    project_id = "linear-theater-442800-s1"
    dataset_id = "STAGING"
    table_id = "product_raw"
    query = ''' SELECT DISTINCT * FROM `linear-theater-442800-s1.STAGING.product_raw`'''

    df_products = run_bigquery_query_to_dataframe(json_key_path=json_key_path, project_id=project_id, dataset_id=dataset_id, table_id=table_id, query=query)

    # Cài đặt và khởi chạy trình duyệt Chrome
    options = webdriver.ChromeOptions()
    options.binary_location = "/usr/bin/google-chrome"
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    # DataFrame để lưu trữ thông tin bán hàng
    df_sales_list = []

    # Duyệt qua từng sản phẩm
    for index, row in df_products.head(5).iterrows():
        product_id = row['Product_ID']
        product_url = row['URL']

        # Mở trang web sản phẩm
        driver.get(product_url)
        driver.set_window_size(1920, 1080)
        time.sleep(5)

        # Cuộn và tải thêm nội dung
        for _ in range(10):
            driver.execute_script("window.scrollBy(0, 1000);")
            time.sleep(2)

        # Nhấn vào phần tử nếu cần
        try:
            clickable_element = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.CLASS_NAME, "class-to-trigger-content"))
            )
            clickable_element.click()
            time.sleep(5)
        except Exception:
            pass

        # Làm mới trang để tải nội dung đầy đủ
        driver.refresh()
        time.sleep(5)

        # Thu thập dữ liệu bán hàng
        try:
            seller_element = driver.find_element(By.CLASS_NAME, "seller-name")
            seller_link = seller_element.find_element(By.XPATH, '//span[@class="seller-name"]//a')
            seller_name = seller_element.text
            seller_url = seller_link.get_attribute('href')
        except Exception:
            seller_name, seller_url = "N/A", "N/A"

        try:
            quantity_sold_element = driver.find_element(By.XPATH, "//div[contains(text(), 'Đã bán')]")
            quantity_sold_text = quantity_sold_element.text.strip()
            quantity_sold = int(quantity_sold_text.split(" ")[-1])
        except Exception:
            quantity_sold = 0

        # Lưu dữ liệu vào danh sách
        df_sales_list.append({
            'Product_ID': product_id,
            'Seller_Name': seller_name,
            'Seller_URL': seller_url,
            'Quantity_Sold': quantity_sold,
            'Crawl_Date': (datetime.now() + timedelta(hours=7)).strftime('%Y-%m-%d')
        })

    # Chuyển danh sách thành DataFrame
    df_sales = pd.DataFrame(df_sales_list)

    if not df_sales.empty:
        df_sales.to_csv(sys.stdout, index=False, encoding="utf-8-sig")
    else:
        print("No products fetched.", file=sys.stderr)

if __name__ == "__main__":
    get_data_and_upload()