from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
from google.cloud import bigquery
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import re
from requests.adapters import HTTPAdapter 
from urllib3.util.retry import Retry
import pandas as pd
import time
from google.cloud import bigquery
from sqlalchemy import create_engine, text
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy import create_engine
from sqlalchemy.types import NVARCHAR
import pandas as pd
import pyodbc

from sqlalchemy.types import NVARCHAR, Float, DateTime,Date
# Hàm kiểm tra kết nối đến SQL Server
# Hàm kiểm tra kết nối đến SQL Serverd

def clear_table(database_name, schema_name, table_name):
    server_name = '172.30.112.1'  # Thay bằng tên server của bạn
    username = "data"
    password = "data123"
    connection_string = (
        f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}"
        f"?driver=ODBC+Driver+17+for+SQL+Server"
    )
    try:
        # Tạo engine kết nối
        engine = create_engine(connection_string, fast_executemany=True)
        
        with engine.connect() as conn:
            # Xây dựng tên bảng đầy đủ với database, schema và table
            full_table_name = f"[{database_name}].[{schema_name}].[{table_name}]"
            # Xóa dữ liệu trong bảng
            print(f"Đang xóa dữ liệu trong bảng {full_table_name}...")
            delete_query = f"DELETE FROM {full_table_name}"
            conn.execute(delete_query)
            print(f"Đã xóa dữ liệu trong bảng {full_table_name}.")
    except Exception as e:
        print(f"Lỗi khi xóa dữ liệu trong bảng {database_name}.{schema_name}.{table_name}: {e}")

def task_product_clean():
    json_key_path = '/opt/airflow/dags/linear-theater-442800-s1-26e4d70ab28a.json'
    query = ''' 
    SELECT DISTINCT 
        Product_ID AS Id,
        Name AS name,
        Price AS price,
        Rating AS rating,
        Data_Date AS crawl_date,
        Created_Time AS created_at,
        Inventory AS warehouse
    FROM `linear-theater-442800-s1.STAGING.product_raw`
    WHERE Inventory IS NOT NULL
    '''
    client = bigquery.Client.from_service_account_json(json_key_path)
    df = client.query(query).to_dataframe()
    server_name = '172.30.112.1'  # Thay bằng tên server của bạn
    database_name = 'CleanedData'            # Thay bằng tên database của bạn
    table_name = 'Product_clean'      # Tên bảng
    username = "data"
    password = "data123"
    def push_to_sqlserver(dataframe, table_name, server_name, database_name):
        try:
            # Đảm bảo mọi dữ liệu đều là chuỗi trước khi đẩy vào SQL Server
            dataframe = dataframe.applymap(lambda x: str(x) if pd.notnull(x) else None)

            # Chuỗi kết nối với Windows Authentication
            connection_string = (
                f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}"
                f"?driver=ODBC+Driver+17+for+SQL+Server"
            )
            engine = create_engine(connection_string, fast_executemany=True)

            # Đẩy dữ liệu vào SQL Server
            dataframe.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',
                index=False,
                dtype={
                    'Id': NVARCHAR(None),
                    'Name': NVARCHAR(None),
                    'warehouse': NVARCHAR(None),
                    'discribe' :NVARCHAR(None),
                    'danhmuc' :NVARCHAR(None),
                    'crawl_date': Date,
                'created_at': Date,
                'rating': Float,
                'discount': Float,
                'price': Float
                }
            )
            print(f"Đã đẩy dữ liệu thành công vào bảng {table_name} trong database {database_name}.")
        except Exception as e:
            print(f"Lỗi khi đẩy dữ liệu vào SQL Server: {e}")
    clear_table( database_name= database_name,  schema_name='dbo', table_name = table_name)
    push_to_sqlserver(df, table_name, server_name, database_name)



def task_comment_clean():
    json_key_path = '/opt/airflow/dags/linear-theater-442800-s1-26e4d70ab28a.json'
    query = ''' SELECT 
    distinct 
    Product_id,
    User,
    Time_on_Platform,
    Title,
    Comment_Date,
    Comment,
    Rating
    FROM `linear-theater-442800-s1.STAGING.comment_raw`
    '''
    client = bigquery.Client.from_service_account_json(json_key_path)
    df = client.query(query).to_dataframe()
    server_name = '172.30.112.1'  # Thay bằng tên server của bạn
    database_name = 'CleanedData'            # Thay bằng tên database của bạn
    table_name = 'Comment_clean'      # Tên bảng
    username = "data"
    password = "data123"
    def push_to_sqlserver(dataframe, table_name, server_name, database_name):
        try:
            # Đảm bảo mọi dữ liệu đều là chuỗi trước khi đẩy vào SQL Server
            dataframe = dataframe.applymap(lambda x: str(x) if pd.notnull(x) else None)

            # Chuỗi kết nối với Windows Authentication
            connection_string = (
                f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}"
                f"?driver=ODBC+Driver+17+for+SQL+Server"
            )
            engine = create_engine(connection_string, fast_executemany=True)

            # Đẩy dữ liệu vào SQL Server
            dataframe.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',
                index=False,
                dtype={
                    'Product_id': NVARCHAR(None),
                    'User': NVARCHAR(None),
                    'Time_on_Platform': NVARCHAR(None),
                    'Title' :NVARCHAR(None),
                    'Comment_Date' :NVARCHAR(None),
                    'Comment':NVARCHAR(None),
                    'Rating': Float
            
                }
            )
            print(f"Đã đẩy dữ liệu thành công vào bảng {table_name} trong database {database_name}.")
        except Exception as e:
            print(f"Lỗi khi đẩy dữ liệu vào SQL Server: {e}")
    clear_table( database_name= database_name,  schema_name='dbo', table_name = table_name)
    push_to_sqlserver(df, table_name, server_name, database_name)



def task_shop_clean():
    json_key_path = '/opt/airflow/dags/linear-theater-442800-s1-26e4d70ab28a.json'
    query = ''' SELECT 
    distinct 
    Product_id,
    Shop_Name,
    Shop_URL,
    Review_Count
    FROM `linear-theater-442800-s1.STAGING.shop_raw`
    where Shop_Name <> "N/A"
    '''
    client = bigquery.Client.from_service_account_json(json_key_path)
    df = client.query(query).to_dataframe()
    server_name = '172.30.112.1'  # Thay bằng tên server của bạn
    database_name = 'CleanedData'            # Thay bằng tên database của bạn
    table_name = 'Shop_clean'      # Tên bảng
    username = "data"
    password = "data123"
    def push_to_sqlserver(dataframe, table_name, server_name, database_name):
        try:
            # Đảm bảo mọi dữ liệu đều là chuỗi trước khi đẩy vào SQL Server
            dataframe = dataframe.applymap(lambda x: str(x) if pd.notnull(x) else None)

            # Chuỗi kết nối với Windows Authentication
            connection_string = (
                f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}"
                f"?driver=ODBC+Driver+17+for+SQL+Server"
            )
            engine = create_engine(connection_string, fast_executemany=True)

            # Đẩy dữ liệu vào SQL Server
            dataframe.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',
                index=False,
                dtype={
                    'Product_id': NVARCHAR(None),
                    'Shop_Name': NVARCHAR(None),
                    'Shop_URL': NVARCHAR(None),
                    'Review_Count' :NVARCHAR(None)
                }
            )
            print(f"Đã đẩy dữ liệu thành công vào bảng {table_name} trong database {database_name}.")
        except Exception as e:
            print(f"Lỗi khi đẩy dữ liệu vào SQL Server: {e}")
    clear_table( database_name= database_name,  schema_name='dbo', table_name = table_name)
    push_to_sqlserver(df, table_name, server_name, database_name)



def task_sale_clean():
    json_key_path = '/opt/airflow/dags/linear-theater-442800-s1-26e4d70ab28a.json'
    query = ''' SELECT 
    distinct 
    Product_id,
    seller_name,
    seller_url,
    Quantity_Sold,
    Crawl_Date
    FROM `linear-theater-442800-s1.STAGING.sale_raw`
    where seller_name <> "N/A"
    '''
    client = bigquery.Client.from_service_account_json(json_key_path)
    df = client.query(query).to_dataframe()
    server_name = '172.30.112.1'  # Thay bằng tên server của bạn
    database_name = 'CleanedData'            # Thay bằng tên database của bạn
    table_name = 'Sale_clean'      # Tên bảng
    username = "data"
    password = "data123"
    def push_to_sqlserver(dataframe, table_name, server_name, database_name):
        try:
            # Đảm bảo mọi dữ liệu đều là chuỗi trước khi đẩy vào SQL Server
            dataframe = dataframe.applymap(lambda x: str(x) if pd.notnull(x) else None)

            # Chuỗi kết nối với Windows Authentication
            connection_string = (
                f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}"
                f"?driver=ODBC+Driver+17+for+SQL+Server"
            )
            engine = create_engine(connection_string, fast_executemany=True)

            # Đẩy dữ liệu vào SQL Server
            dataframe.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',
                index=False,
                dtype={
                    'Product_id': NVARCHAR(None),
                    'seller_name': NVARCHAR(None),
                    'seller_url': NVARCHAR(None),
                    'Quantity_Sold' :Float,
                    'Crawl_Date':Date
                }
            )
            print(f"Đã đẩy dữ liệu thành công vào bảng {table_name} trong database {database_name}.")
        except Exception as e:
            print(f"Lỗi khi đẩy dữ liệu vào SQL Server: {e}")
    clear_table( database_name= database_name,  schema_name='dbo', table_name = table_name)
    push_to_sqlserver(df, table_name, server_name, database_name)

def read_from_sqlserver(query):
    server_name = '172.30.112.1'  # Thay bằng tên server của bạn
    database_name = 'OLAP'            # Thay bằng tên database của bạn
    username = "data"
    password = "data123"
    # Chuỗi kết nối tới SQL Server
    connection_string = (
        f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}"
        f"?driver=ODBC+Driver+17+for+SQL+Server"
    )
    
    # Tạo engine
    engine = create_engine(connection_string)
    
    # Thực thi query và đọc dữ liệu vào DataFrame
    df = pd.read_sql(query, con=engine)
    print("Đã đọc dữ liệu từ SQL Server thành công.")
    return df   


def  task_dim_warehouse():
    query = '''select distinct
      [warehouse]
    FROM [CleanedData].[dbo].[Product_clean]
    WHERE [warehouse] IS NOT NULL
    '''
    df = read_from_sqlserver(query)
    df['w_id'] = range(1, len(df) + 1)

    def push_to_sqlserver(dataframe, table_name, if_exists='append'):
        server_name = '172.30.112.1'  # Thay bằng tên server của bạn
        database_name = 'OLAP'            # Thay bằng tên database của bạn
        username = "data"
        password = "data123"    
        try:
            # Chuỗi kết nối SQL Server
            connection_string = (
                f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}"
                f"?driver=ODBC+Driver+17+for+SQL+Server"
            )
            
            # Tạo engine kết nối
            engine = create_engine(connection_string, fast_executemany=True)
          
            # Đẩy dữ liệu vào SQL Server
            dataframe.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=False
            )
            print(f"Đã đẩy dữ liệu thành công vào bảng {table_name} trong database {database_name}.")
        except Exception as e:
            print(f"Lỗi khi đẩy dữ liệu vào SQL Server: {e}")
    clear_table( database_name='OLAP',  schema_name='dbo', table_name= 'Dim_warehouse')
    push_to_sqlserver(df, table_name = 'Dim_warehouse', if_exists='append')


def task_dim_crawl_date():
    query = '''
    SELECT DISTINCT 
        CAST(Crawl_Date AS DATE) AS DateKey,                       
        YEAR(CAST(Crawl_Date AS DATE)) AS Year,                   
        MONTH(CAST(Crawl_Date AS DATE)) AS Month,                 
        DAY(CAST(Crawl_Date AS DATE)) AS Day,                     
        DATENAME(WEEKDAY, CAST(Crawl_Date AS DATE)) AS DayName,   
        DATEPART(QUARTER, CAST(Crawl_Date AS DATE)) AS Quarter,  
        DATEPART(WEEK, CAST(Crawl_Date AS DATE)) AS Week,         
        CASE 
            WHEN DATEPART(WEEKDAY, CAST(Crawl_Date AS DATE)) IN (1, 7) THEN 'Weekend' 
            ELSE 'Weekday'                                                           
        END AS IsWeekend                                                             
    FROM CleanedData.dbo.Sale_clean
    ORDER BY DateKey;
    '''
    df = read_from_sqlserver(query)
    def push_to_sqlserver(dataframe, table_name, if_exists='append'):
        server_name = '172.30.112.1'  # Thay bằng tên server của bạn
        database_name = 'OLAP'            # Thay bằng tên database của bạn
        username = "data"
        password = "data123"    
        try:
            # Chuỗi kết nối SQL Server
            connection_string = (
                f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}"
                f"?driver=ODBC+Driver+17+for+SQL+Server"
            )
            
            # Tạo engine kết nối
            engine = create_engine(connection_string, fast_executemany=True)

            # Đẩy dữ liệu vào SQL Server
            dataframe.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=False
            )
            print(f"Đã đẩy dữ liệu thành công vào bảng {table_name} trong database {database_name}.")
        except Exception as e:
            print(f"Lỗi khi đẩy dữ liệu vào SQL Server: {e}")
    clear_table( database_name='OLAP',  schema_name='dbo', table_name='Dim_CrawlDate')
    push_to_sqlserver(df, table_name = 'Dim_CrawlDate', if_exists='append')


def  task_dim_product():
    query = '''
    SELECT Distinct
        a.[Id]
        ,a.[name]
        ,b.w_id
    FROM [CleanedData].[dbo].[Product_clean] a
    left join OLAP.dbo.Dim_warehouse b on a.warehouse = b.warehouse
    '''
    def push_to_sqlserver(dataframe, table_name, if_exists='append'):
        server_name = '172.30.112.1'  # Thay bằng tên server của bạn
        database_name = 'OLAP'            # Thay bằng tên database của bạn
        username = "data"
        password = "data123"   
        try:
            # Chuỗi kết nối SQL Server
            connection_string = (
                f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}"
                f"?driver=ODBC+Driver+17+for+SQL+Server"
            )
            
            # Tạo engine kết nối
            engine = create_engine(connection_string, fast_executemany=True)

            # Đẩy dữ liệu vào SQL Server
            dataframe.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=False,
                dtype={
                    'name': NVARCHAR(None)
                }
            )
            print(f"Đã đẩy dữ liệu thành công vào bảng {table_name} trong database {database_name}.")
        except Exception as e:
            print(f"Lỗi khi đẩy dữ liệu vào SQL Server: {e}")
    df = read_from_sqlserver(query)
    clear_table( database_name='OLAP',  schema_name='dbo', table_name='Dim_product')
    push_to_sqlserver(df, table_name = 'Dim_product', if_exists='append')

def  task_dim_user():
    query = '''SELECT distinct 
      [User]
      ,[Time_on_Platform]
    FROM [CleanedData].[dbo].[Comment_clean]
'''
    df = read_from_sqlserver(query)
    df['user_id'] = range(1, len(df) + 1)
    def push_to_sqlserver(dataframe, table_name, if_exists='append'):
        server_name = '172.30.112.1'  # Thay bằng tên server của bạn
        database_name = 'OLAP'            # Thay bằng tên database của bạn
        username = "data"
        password = "data123"   
        try:
            # Chuỗi kết nối SQL Server
            connection_string = (
                f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}"
                f"?driver=ODBC+Driver+17+for+SQL+Server"
            )
            
            # Tạo engine kết nối
            engine = create_engine(connection_string, fast_executemany=True)

            # Đẩy dữ liệu vào SQL Server
            dataframe.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=False,
                dtype={
                    'Time_on_Platform': NVARCHAR(None),
                    'User': NVARCHAR(None)
                }
            )
            print(f"Đã đẩy dữ liệu thành công vào bảng {table_name} trong database {database_name}.")
        except Exception as e:
            print(f"Lỗi khi đẩy dữ liệu vào SQL Server: {e}")
    clear_table( database_name='OLAP',  schema_name='dbo', table_name='Dim_user')
    push_to_sqlserver(df, table_name = 'Dim_user', if_exists='append')




def  task_dim_shop():
    def push_to_sqlserver(dataframe, table_name):
        server_name = '172.30.112.1'  # Thay bằng tên server của bạn
        database_name = 'OLAP'          # Thay bằng tên database của bạn
        username = "data"
        password = "data123"
        connection_string = (
            f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}"
            f"?driver=ODBC+Driver+17+for+SQL+Server"
        )
        try:
            # Tạo engine kết nối
            engine = create_engine(connection_string, fast_executemany=True)

            # Đẩy dữ liệu vào SQL Serve
            dataframe.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',  # Chỉ thêm dữ liệu sau khi bảng đã được làm sạch
                index=False,
                dtype={
                    'Shop_Name': NVARCHAR(None),
                    'Review_Count': NVARCHAR(None),
                   
                }
            )
            print(f"Đã đẩy dữ liệu thành công vào bảng {table_name} trong database {database_name}.")
        except Exception as e:
            print(f"Lỗi khi đẩy dữ liệu vào SQL Server: {e}")

    # Ví dụ sử dụng:
   
    clear_table( database_name='OLAP',  schema_name='dbo', table_name='Dim_shop')
    query = '''SELECT DISTINCT 
                [Shop_Name], 
                [Shop_URL], 
                [Review_Count] 
            FROM [CleanedData].[dbo].[Shop_clean]'''
    df = read_from_sqlserver(query)
    df['shop_id'] = range(1, len(df) + 1)
    push_to_sqlserver(df, table_name='Dim_shop')


def  task_fact_sale():
    query = '''SELECT distinct [Product_id],
      b.shop_id
      ,[Quantity_Sold]
      ,[Crawl_Date]
  FROM [CleanedData].[dbo].[Sale_clean] a
  left join OLAP.dbo.Dim_shop b on a.seller_url = b.shop_url
    '''
    df = read_from_sqlserver(query)
    df['shop_id'] = range(1, len(df) + 1)
    def push_to_sqlserver(dataframe, table_name, if_exists='append'):
        server_name = '172.30.112.1'  # Thay bằng tên server của bạn
        database_name = 'OLAP'            # Thay bằng tên database của bạn
        username = "data"
        password = "data123"   
        try:
            # Chuỗi kết nối SQL Server
            connection_string = (
                f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}"
                f"?driver=ODBC+Driver+17+for+SQL+Server"
            )
            # Tạo engine kết nối
            engine = create_engine(connection_string, fast_executemany=True)
            # Đẩy dữ liệu vào SQL Server
            dataframe.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=False
            )
            print(f"Đã đẩy dữ liệu thành công vào bảng {table_name} trong database {database_name}.")
        except Exception as e:
            print(f"Lỗi khi đẩy dữ liệu vào SQL Server: {e}")
    clear_table( database_name='OLAP',  schema_name='dbo', table_name='Fact_sale')
    push_to_sqlserver(df, table_name = 'Fact_sale', if_exists='append')


def  task_fact_comment():
    query = '''SELECT distinct [Product_id]
      ,b.user_id
      ,[Title]
      ,[Comment_Date]
      ,[Comment]
      ,[Rating]
  FROM [CleanedData].[dbo].[Comment_clean] a
  left join OLAP.dbo.Dim_user b on a.[user] =  b.[user]
'''
    df = read_from_sqlserver(query)
    df['shop_id'] = range(1, len(df) + 1)
    def push_to_sqlserver(dataframe, table_name, if_exists='append'):
        server_name = '172.30.112.1'  # Thay bằng tên server của bạn
        database_name = 'OLAP'            # Thay bằng tên database của bạn
        username = "data"
        password = "data123"   
        try:
            # Chuỗi kết nối SQL Server
            connection_string = (
                f"mssql+pyodbc://{username}:{password}@{server_name}/{database_name}"
                f"?driver=ODBC+Driver+17+for+SQL+Server"
            )
            
            # Tạo engine kết nối
            engine = create_engine(connection_string, fast_executemany=True)
            # Đẩy dữ liệu vào SQL Server
            dataframe.to_sql(
                name=table_name,
                con=engine,
                if_exists=if_exists,
                index=False,
                dtype={
                    'Title': NVARCHAR(None),
                    'Comment_Date': NVARCHAR(None),
                    'Comment' :NVARCHAR(None),
                }
            )
            print(f"Đã đẩy dữ liệu thành công vào bảng {table_name} trong database {database_name}.")
        except Exception as e:
            print(f"Lỗi khi đẩy dữ liệu vào SQL Server: {e}")
    clear_table( database_name='OLAP',  schema_name='dbo', table_name='Fact_comment')
    push_to_sqlserver(df, table_name = 'Fact_comment', if_exists='append')



dag = DAG(
    'test_bigquery_dag_full',
    default_args={
        'retries': 2,
        'retry_delay': timedelta(minutes=0.5),
        'start_date': days_ago(1)
    },
    schedule_interval=None,
    catchup=False
)

# Định nghĩa PythonOperator để thực thi tác vụ

Shop_clean_and_push_to_SQL = PythonOperator(
    task_id='Shop_clean_and_push_to_SQL',
    python_callable=task_shop_clean,
    dag=dag
)

Sale_clean_and_push_to_SQL = PythonOperator(
    task_id='Sale_clean_and_push_to_SQL',
    python_callable=task_sale_clean,
    dag=dag
)


Comment_clean_and_push_to_SQL = PythonOperator(
    task_id='Comment_clean_and_push_to_SQL',
    python_callable=task_comment_clean,
    dag=dag
)


Prodcut_clean_and_push_to_SQL = PythonOperator(
    task_id='Product_clean_and_push_to_SQL' ,
    python_callable=task_product_clean,
    dag=dag
)


Dim_warehouse = PythonOperator(
    task_id='Dim_warehouse',
    python_callable=task_dim_warehouse,
    dag=dag
)

Dim_crawldate = PythonOperator(
    task_id='Dim_CrawlDate',
    python_callable=task_dim_crawl_date,
    dag=dag
)

Dim_Products = PythonOperator(
    task_id='Dim_Products',
    python_callable=task_dim_product,
    dag=dag
)

Dim_Shops  = PythonOperator(
    task_id='Dim_Shops',
    python_callable=task_dim_shop,
    dag=dag
)

Dim_Users = PythonOperator(
    task_id='Dim_Users',
    python_callable=task_dim_user,
    dag=dag
)

Fact_Comments = PythonOperator(
    task_id='Fact_Comments',
    python_callable=task_fact_comment,
    dag=dag
)


Fact_Sales = PythonOperator(
    task_id='Fact_Sales',
    python_callable=task_fact_sale,
    dag=dag
)

Shop_clean_and_push_to_SQL >> Dim_Shops
Prodcut_clean_and_push_to_SQL >> Dim_warehouse 
[Prodcut_clean_and_push_to_SQL, Dim_warehouse]>> Dim_Products
Comment_clean_and_push_to_SQL >> Dim_Users
Sale_clean_and_push_to_SQL >> Dim_crawldate


dim_tasks_sale = [Dim_Shops, Dim_Products, Dim_Users, Sale_clean_and_push_to_SQL, Dim_crawldate]
dim_tasks_comment = [Dim_Shops, Dim_Products, Dim_Users, Comment_clean_and_push_to_SQL]
# fact_tasks = [Fact_Comments, Fact_Sales]

# Sử dụng vòng lặp để thiết lập dependency giữa từng task dim và từng task fact
# for dim_task in dim_tasks_sale:
#     for fact_task in fact_tasks:
#         dim_task >> fact_task

dim_tasks_sale >> Fact_Sales
dim_tasks_comment >>Fact_Comments
