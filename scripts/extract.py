import pandas
import geopandas
import json
import requests
import os
import datetime
import pytz


pandas.set_option('display.max_columns', None)
pandas.set_option('display.width', 1000)

SEPARATOR = "=" * 80
SUB_SEPARATOR = "-" * 80

# ----- extract geojson -----
def extract_geojson(url, **kwargs):

    ti = kwargs['ti']
    try:
        # url = r'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson'

        # ดึงข้อมูลจาก URL (string)
        get_url_data = requests.get(url)
        get_url_data.raise_for_status()

        # แปลงข้อมูลที่ได้เป็น json
        json_data = get_url_data.json()
        
        # ดึงข้อมูลในส่วนของ Features
        features_list = json_data.get('features', [])

        # แปลงข้อมูลให้อยู่ในรูปแบบ Dataframe
        geo_df = geopandas.GeoDataFrame.from_features(features_list)


        # ------------------------------------------------

        print(SEPARATOR)
        print(f"--- ขั้นตอนการ : ดึงข้อมูลแผ่นดินไหวจาก URL ---")
        print(SEPARATOR)
        print(f"URL ที่ใช้ดึงข้อมูล: {url}")
        print(f"สถานะการตอบกลับ: {get_url_data.status_code}")
        print(f"จำนวนเหตุการณ์ที่ดึงมาได้: {len(features_list)} เหตุการณ์")

        # ------------------------------------------------

        print(f"\n{SUB_SEPARATOR}")
        print(f"--- ตัวอย่างข้อมูล GeoDataFrame ---")
        print(SUB_SEPARATOR)
        print(f"{geo_df.head(5)}")

        # ------------------------------------------------

        print(f"\n{SUB_SEPARATOR}")
        print("--- ข้อมูลโครงสร้างตาราง ---")
        print(SUB_SEPARATOR)
        print(f"{geo_df.info()}")

        # ------------------------------------------------


        # กำหนด Base Path
        base_path = f"/opt/airflow/data/"
        raw_dir = os.path.join(base_path,'raw')

        # สร้างโฟลเดอร์สำหรับเก็บข้อมูลดิบจาก URL 
        os.makedirs(raw_dir, exist_ok=True)

        # กำหนดวันที่ในการบันทึกไฟล์
        thailand_tz = pytz.timezone('Asia/Bangkok')
        date_execution_date = datetime.datetime.now(thailand_tz).strftime("%Y-%m-%dTT%H%M%S")

        # ตั้งชื่อไฟล์
        file_name = f"{date_execution_date}_raw_earthquakes.parquet"

        # กำหนด Path ของไฟล์ที่ต้องการบันทึก
        raw_file_path = os.path.join(raw_dir, file_name)

        # ข้อมูลที่จะเอามาบันทึก
        geo_df.to_parquet(raw_file_path)

        ti.xcom_push(key='raw_file_path', value=raw_file_path)

      
       # ------------------------------------------------

        print(f"\n{SEPARATOR}")
        print("--- บันทึกข้อมูล ---")
        print(SEPARATOR)
        print(f"โฟลเดอร์สำหรับบันทึก: {raw_dir}")
        print(f"ชื่อไฟล์ที่บันทึก: {file_name}")
        print(f"บันทึกข้อมูล GeoDataFrame สำเร็จ")
        print(SEPARATOR)

       # ------------------------------------------------


    except Exception as e:

        print(f"เกิดข้อผิดพลาดที่ def extract_geojson(url, **kwargs): {e}")
        ti.xcom_push(key='raw_file_path', value=None)
        raise
      
# api_data = extract_geojson(url)