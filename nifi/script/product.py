from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.cloud import bigquery
import warnings
import logging
import json
# Tắt các cảnh báo không cần thiết
warnings.simplefilter(action='ignore', category=FutureWarning)
logging.getLogger("urllib3").setLevel(logging.ERROR)

def get_data_and_upload():
    # URL cơ bản cho API của Tiki để lấy danh sách sản phẩm
    base_url = "https://tiki.vn/api/personalish/v1/blocks/listings?limit=40&include=advertisement&aggregations=2&trackity_id=fb703a37-2f3d-2956-96ac-bd90536bb792&category={}&page="

    # Headers để giả lập yêu cầu từ trình duyệt
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    }

    # Tạo một session với cơ chế retry
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
        backoff_factor=1
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # Hàm lấy danh sách sản phẩm từ Tiki
    def crawl_tiki_data(category_id, max_products=1000):
        product_data = []
        page_num = 1
        while len(product_data) < max_products:
            url = base_url.format(category_id) + str(page_num)
            try:
                response = session.get(url, headers=headers)
                if response.status_code != 200:
                    break
                data = response.json().get('data', [])
                if not data:
                    break
                for item in data:
                    product_id = item.get('id')
                    product_url = f"https://tiki.vn/{item.get('url_path')}"

                    product_data.append({
                        "Id": product_id,
                        "Tên": item.get('name'),
                        "Giá": item.get('price'),
                        "URL": product_url,
                        "Sao đánh giá": item.get('rating_average'),
                        "Ngày lấy dữ liệu": (datetime.now() + timedelta(hours=7)).strftime('%Y-%m-%d'),
                    })
                    if len(product_data) >= max_products:
                        break
                page_num += 1
            except requests.exceptions.RequestException:
                break
        return product_data

    # Hàm lấy thông tin chi tiết sản phẩm
    def get_product_details(product_id):
        details_url = f"https://tiki.vn/api/v2/products/{product_id}"
        try:
            response = session.get(details_url, headers=headers)
            if response.status_code == 200:
                product_details = response.json()
                return {
                    "Thời gian tạo sản phẩm": product_details.get('created_at'),
                    "Kho hàng": product_details.get('inventory', {}).get('fulfillment_type', None),
                    "Mô tả ngắn": product_details.get('short_description', None),
                    "Giảm giá": product_details.get('discount', None),
                    "Danh mục": json.dumps(product_details.get('categories', []))
                }
            else:
                return {}
        except requests.exceptions.RequestException:
            return {}

    # Chọn ID danh mục và số lượng sản phẩm cần lấy
    category_id = '1789'  # Thay thế bằng ID danh mục mong muốn
    max_products = 200  # Số lượng sản phẩm để lấy (có thể điều chỉnh)

    # Lấy dữ liệu danh sách sản phẩm
    products = crawl_tiki_data(category_id, max_products)

    # Lấy thông tin chi tiết sản phẩm và bổ sung vào danh sách
    for product in products:
        details = get_product_details(product['Id'])
        product.update(details)

    # Tạo DataFrame từ danh sách sản phẩm
    df_products = pd.DataFrame(products)

    # Định dạng lại dữ liệu cho phù hợp với schema BigQuery
    """
    if not df_products.empty:
        df_products['Id'] = df_products['Id'].astype('Int64')  # INTEGER
        df_products['Giá'] = df_products['Giá'].fillna(0).astype('Int64')  # INTEGER
        df_products['Sao đánh giá'] = df_products['Sao đánh giá'].fillna(0).astype('float64')  # FLOAT
        df_products['Ngày lấy dữ liệu'] = pd.to_datetime(df_products['Ngày lấy dữ liệu']).dt.strftime('%Y-%m-%d')  # DATE (chuỗi)
        df_products['Thời gian tạo sản phẩm'] = df_products['Thời gian tạo sản phẩm'].fillna(0).astype('Int64')  # INTEGER
        df_products['Kho hàng'] = df_products['Kho hàng'].astype(str)  # STRING
        df_products['Mô tả ngắn'] = df_products['Mô tả ngắn'].astype(str)  # STRING
        df_products['Giảm giá'] = df_products['Giảm giá'].fillna(0).astype('Int64')  # INTEGER
        # Xử lý cột "Danh mục" để đảm bảo đúng định dạng
        if 'Danh mục' in df_products.columns:
            df_products['Danh mục'] = df_products['Danh mục'].apply(
                lambda x: json.loads(x) if isinstance(x, str) and x.startswith("[") else []
            )
   
    if not df_products.empty:
    # Chuyển toàn bộ các cột trong DataFrame sang kiểu string
        df_products = df_products.astype(str)

    # Đảm bảo xử lý cột "Danh mục" để giữ đúng định dạng JSON string
    if 'Danh mục' in df_products.columns:
        df_products['Danh mục'] = df_products['Danh mục'].apply(
            lambda x: json.loads(x) if isinstance(x, str) and x.startswith("[") else "[]"
        )
    """
    

     # Chuyển tất cả các cột sang kiểu string để đảm bảo định dạng nhất quán
    

    if not df_products.empty:
        df_products = df_products.rename(columns={
        "Id": "Product_ID",
        "Tên": "Name",
        "Giá": "Price",
        "URL": "URL",
        "Sao đánh giá": "Rating",
        "Ngày lấy dữ liệu": "Data_Date",
        "Thời gian tạo sản phẩm": "Created_Time",
        "Kho hàng": "Inventory",
        "Mô tả ngắn": "Description",
        "Giảm giá": "Discount",
        "Danh mục": "Categories"
    })
        df_products.to_csv(sys.stdout, index=False, encoding="utf-8-sig")
    else:
        print("No products fetched.", file=sys.stderr)

if __name__ == "__main__":
    get_data_and_upload()
    
