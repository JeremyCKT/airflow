from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import requests
import mysql.connector
import sendgrid
from sendgrid.helpers.mail import Mail, Email, To, Content

def send_email_via_sendgrid(to_email, subject, content):
    sg = sendgrid.SendGridAPIClient(api_key='xxxxxx')
    from_email = Email("xxx@xxx.xxx")  # 修改為您的寄件者email地址
    to_email = To(to_email)
    content = Content("text/plain", content)
    mail = Mail(from_email, to_email, subject, content)
    response = sg.client.mail.send.post(request_body=mail.get())

def scrape_and_write_to_mysql():
    url = "https://beri.tw"
    endpoint = f"{url}/wp-json/wp/v2/posts?per_page=100"
    resp = requests.get(endpoint)
    total_post = int(resp.headers["x-wp-total"])
    total_pages = int(resp.headers["x-wp-totalpages"])

    # MySQL連線設定
    db_config = {
        "host": "xxx.xxx.xxx.xxx",
        "user": "xxxxxx",
        "password": "xxxxxx",
        "database": "xxxxxx"
    }

    # 建立MySQL連線
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor()

    # 建立資料表 (如果尚未建立)
    create_table_query = """
    CREATE TABLE IF NOT EXISTS WPScraper2 (
        title VARCHAR(255),
        date DATETIME,
        link VARCHAR(255)
    )
    """
    cursor.execute(create_table_query)
    conn.commit()

    for page in range(1, 5):  # 若要取得所有頁面，請改回 range(1, total_pages+1)
        if page > 1:
            endpoint = f"{url}/wp-json/wp/v2/posts?per_page=100&page={page}"
            resp = requests.get(endpoint)

        for item in resp.json():
            title = item["title"]["rendered"]
            date = item["date"]
            link = item["link"]
            #print(title)
            #print(date)
            #print(link)

            # 將資料寫入MySQL資料表中
            insert_query = "INSERT INTO WPScraper2 (title, date, link) VALUES (%s, %s, %s)"
            data = (title, date, link)
            cursor.execute(insert_query, data)
            conn.commit()

    # 關閉資料庫連線
    cursor.close()
    conn.close()

    to_email = "xxx@xxx.xxx"  # 收件者email
    subject = "WordPress Crawler Completed"  # 郵件主題
    content = "The WordPress crawler has completed successfully!"  # 郵件內容
    send_email_via_sendgrid(to_email, subject, content)

# 定義Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 25),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'WPArticle2',
    default_args=default_args,
    description='Crawl WordPress posts and write to MySQL',
    schedule_interval='@daily',  # 設定排程，這裡設定每天執行一次
)

# 建立PythonOperator，指定要執行的Python函式
crawl_and_write_task = PythonOperator(
    task_id='crawl_and_write_task',
    python_callable=scrape_and_write_to_mysql,
    dag=dag,
)

crawl_and_write_task
