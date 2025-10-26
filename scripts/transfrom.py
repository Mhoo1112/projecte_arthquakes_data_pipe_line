import pandas
import geopandas
import json
import requests
import os
import datetime
import pytz


SEPARATOR = "=" * 80
SUB_SEPARATOR = "-" * 80
# url = r'https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson'
# ----- transfrom geojson -----
def transfrom_geojson(**kwargs):

    ti = kwargs['ti']
    try:
        path_file_name = kwargs['ti'].xcom_pull(key='raw_file_path',task_ids='extract_geojson')
        
        # ดึงข้อมูล path และชื่อไฟล์
        raw_path_xcom = os.path.dirname(path_file_name)
        file_name_xcom = os.path.basename(path_file_name)


        # ------------------------------------------------

        print(SEPARATOR)
        print(f"--- ขั้นตอนการ : แปลงข้อมูลแผ่นดินไหว ---")
        print(SEPARATOR)
        print(f"อ่านข้อมูลจากโฟลเดอร์: {raw_path_xcom}")
        print(f"อ่านข้อมูลจากไฟล์: {file_name_xcom}")

        # ------------------------------------------------

        # ตรวจสอบข้อมูลว่ามีรึเปล่า
        if not path_file_name:
            print(f"\n{SEPARATOR}")
            print("ไม่พบไฟล์จากการ Extract - ยกเลิก Task Transform")
            print(SEPARATOR)
            raise ValueError("ไม่พบ Path ไฟล์จาก Task 'extract_geojson' ไม่สามารถดำเนินการ Transform ต่อได้")
            # raise ValueError(error_message)


        # อ่านข้อมูลพร้อมแปลงเป็น geopandas data frame
        geo_df = geopandas.read_parquet(path_file_name)

        
        # loop ข้อมูลเพื่อลบช่องว่างข้างหน้าและข้างหลัง ที่มีข้อมูลเป็น object, string
        # ลบอักษร "," ที่อยู่ข้างหน้าของข้อมูล
        for col in geo_df:
            if geo_df[col].dtype == 'object' or geo_df[col].dtype == 'string':
                geo_df[col] = geo_df[col].str.strip().str.lstrip(',')


        # เข้าถึงข้อมูลแต่ละคอลัมน์เพื่อแปลงประเภทของข้อมูล
        geo_df['time'] = pandas.to_datetime(geo_df['time'], unit='ms', utc=True)
        geo_df['updated'] = pandas.to_datetime(geo_df['updated'], unit='ms', utc=True)

        # เปลี่ยนชื่อคอลัมน์
        geo_df['magtype'] = geo_df['magType']

        # เลือกคอลัมน์ที่จะลบ
        columns_to_drop = ['magType']
        geo_df = geo_df.drop(columns_to_drop, axis=1)

        # แปลงชนิดของข้อมูล nst dmin gap
        geo_df['nst'] = pandas.to_numeric(geo_df['nst'], errors='coerce')
        geo_df['nst'] = geo_df['nst'].fillna(0).astype('int64')

        geo_df['dmin'] = pandas.to_numeric(geo_df['dmin'], errors='coerce')
        geo_df['dmin'] = geo_df['dmin'].fillna(0).astype('float64')
        
        geo_df['gap'] = pandas.to_numeric(geo_df['gap'], errors='coerce')
        geo_df['gap'] = geo_df['gap'].fillna(0).astype('float64')
        

                                                # geo_df['time_event_date'] = pandas.to_datetime(geo_df['time'].dt.date)
                                                # geo_df['time_event_hour'] = geo_df['time'].dt.strftime('%H:%M:%S')
                                                # # geo_df['time_event_datetime_str'] = geo_df['time_event_date'].astype(str) + ' ' + geo_df['time_event_hour']
                                                # # eo_df['time_event_datetime'] = pandas.to_datetime(geo_df['time_event_datetime_str'])
                                                # geo_df['updated_date'] = pandas.to_datetime(geo_df['updated'].dt.date)
                                                # geo_df['updated_of_day'] = geo_df['updated'].dt.strftime('%H:%M:%S')

                                                
                                                # # เลือกคอลัมน์ที่จะลบ
                                                # columns_to_drop = ['time', 'updated']


                                                # # ลบคอลัมน์ ['time', 'updated']
                                                # geo_df = geo_df.drop(columns_to_drop, axis=1)
                                                # # cleaned_geo_df = geo_df.drop(columns_to_drop, axis=1, inplace=True/False)
                                                # # geo_df = geo_df.rename(columns={'magType': 'magtype'})


                                                # # ดูรูปแบบข้อมูล
                                                # # ------------------------------------------------

                                                # print(f"\n{SUB_SEPARATOR}")
                                                # print(f"--- ตัวอย่างข้อมูลคอลัมน์ 'time','time_event_date','time_event_hour' ---")
                                                # print(geo_df[['time','time_event_date','time_event_hour']])
                                                # print(SUB_SEPARATOR)

                                                # print(f"\n{SUB_SEPARATOR}")
                                                # print(f"--- ตัวอย่างข้อมูลคอลัมน์ 'updated','updated_date','updated_of_day' ---")
                                                # print(geo_df[['updated','updated_date','updated_of_day']])
                                                # print(SUB_SEPARATOR)

                                                # # ------------------------------------------------


        # เพิ่มคอลัมน์ time_add เวลาที่ถูกบันทึกลงฐานข้อมูล ณ ปัจจุบัน
        tz = pytz.timezone('Asia/Bangkok')
        # geo_df['time_add'] = pandas.to_datetime(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        geo_df['time_add'] = geo_df['time_add'] = datetime.datetime.now(tz)


        # ลบข้อมูลที่ซ้ำกัน
        geo_df = geo_df.drop_duplicates(inplace=False)
        # geo_df.drop_duplicates(inplace=True)


        # แปลงข้อมูลระบบพิกัด
        # ------------------------------------------------

        print(f"\n{SUB_SEPARATOR}")
        print(f"--- แปลงข้อมูลระบบพิกัด geometry ---")
        # แปลงข้อมูล geometry เป็นระบบพิกัด EPSG:4326
        TARGET_CRS = 'EPSG:4326'
        if geo_df.crs is None:
            print("CRS ยังไม่ได้ถูกตั้งค่า กำลังตั้งค่าเป็น EPSG:4326")
            geo_df = geo_df.set_crs(TARGET_CRS, allow_override=True)
        elif geo_df.crs != TARGET_CRS:
            print(f"CRS ปัจจุบันคือ {geo_df.crs.to_string()} กำลังแปลงเป็น EPSG:4326")
            geo_df = geo_df.to_crs(TARGET_CRS)
        else:
            print(f"CRS ถูกต้องแล้ว ({TARGET_CRS}) ไม่ต้องแปลงเป็น EPSG:4326")
        
        print(f"\n{SUB_SEPARATOR}")

        # ------------------------------------------------


        # เลือกข้อมูลที่ต้องการ Load to Postgre
        select_column = ['geometry',
            'mag',
            'place',
            'time',
            'updated',
            'url',
            'detail',
            'status',
            'tsunami',
            'sig',
            'ids',
            'sources',
            'nst',
            'dmin',
            'rms',
            'gap',
            'magtype',
            'type',
            'time_add']

        cleaned_geo_df = geo_df[select_column]


        # ------------------------------------------------

        print(f"\n{SUB_SEPARATOR}")
        print(f"--- ตัวอย่างข้อมูล GeoDataFrame ---")
        print(SUB_SEPARATOR)
        print(cleaned_geo_df.head(5))

        # ------------------------------------------------

        print(f"\n{SUB_SEPARATOR}")
        print("--- ข้อมูลโครงสร้างตาราง ---")
        print(SUB_SEPARATOR)
        print(cleaned_geo_df.info())

        # ------------------------------------------------

        
        # กำหนด Base Path
        base_path = f"/opt/airflow/data/"
        processed_dir = os.path.join(base_path,'processed')

        # สร้างโฟลเดอร์สำหรับเก็บข้อมูลดิบจาก URL 
        os.makedirs(processed_dir, exist_ok=True)

        # กำหนดวันที่ในการบันทึกไฟล์
        thailand_tz = pytz.timezone('Asia/Bangkok')
        date_execution_date = datetime.datetime.now(thailand_tz).strftime("%Y-%m-%dTT%H%M%S")
        # date_execution_date = kwargs['ds']

        # ตั้งชื่อไฟล์
        file_name = f"{date_execution_date}_processed_earthquakes.parquet"

        # กำหนด Path ของไฟล์ที่ต้องการบันทึก
        processed_file_path = os.path.join(processed_dir, file_name)

        # ข้อมูลที่จะเอามาบันทึก
        cleaned_geo_df.to_parquet(processed_file_path)

        ti.xcom_push(key='processed_file_path', value=processed_file_path)

       # ------------------------------------------------

        print(f"\n{SEPARATOR}")
        print("--- บันทึกข้อมูล ---")
        print(SEPARATOR)
        print(f"โฟลเดอร์สำหรับบันทึก: {processed_dir}")
        print(f"ชื่อไฟล์ที่บันทึก: {file_name}")
        print(f"บันทึกข้อมูล GeoDataFrame สำเร็จ")
        print(SEPARATOR)

       # ------------------------------------------------

    except Exception as e:

        print(f"เกิดข้อผิดพลาดที่ def transfrom_geojson(**kwargs): {e}")
        kwargs['ti'].xcom_push(key="processed_file_path", value=None)
        raise
        # raise ValueError(error_message)

# final_data = transfrom_geojson(url)