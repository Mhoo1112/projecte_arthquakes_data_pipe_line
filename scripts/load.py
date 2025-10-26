import pandas
import geopandas
import json
import requests
import os
import sqlalchemy
import datetime
import pytz


SEPARATOR = "=" * 80
SUB_SEPARATOR = "-" * 80
# ----- load geojson -----
def load_geojson(database_url, **kwargs):
    ti = kwargs['ti']
    processed_file_path = kwargs['ti'].xcom_pull(key='processed_file_path', task_ids='transfrom_geojson')
    

    # ตรวจสอบข้อมูลว่ามีรึเปล่า
    if not processed_file_path:
        print(f"\n{SEPARATOR}")
        print("ไม่พบไฟล์จากการ Transfrom - ยกเลิก Task Load")
        print(SEPARATOR)
        raise ValueError("ไม่พบ Path ไฟล์จาก Task 'transfrom_geojson' ไม่สามารถดำเนินการ Load ต่อได้")
        # raise ValueError(error_message)

    try:
        # สร้างตัวแปรเชื่อต่อกับฐานข้อมูล Postgre
        engine = sqlalchemy.create_engine(database_url)


        # ------------------------------------------------

        print(SEPARATOR)
        print(f"--- ขั้นตอนการ : โหลดข้อมูลแผ่นดินไหวไปที่ Postgre/PostGIS ---")
        print(SEPARATOR)
        print(f"เชื่อมต่อฐานข้อมูลสำเร็จ")

        # ------------------------------------------------


        # อ่านข้อมูลจากไฟล์ที่แปลงข้อมูลเรียบร้อยแล้ว
        processed_file = geopandas.read_parquet(processed_file_path)

        # กำหนดชื่อ ตาราง
        TABLE_NAME = 'earthquakes'

        # โหลดข้อมูลลง PostgreSQL/PostGIS
        processed_file.to_postgis(
            name = TABLE_NAME,
            con = engine,
            if_exists = 'append',
            index = False,
            # crs = processed_file.crs.to_string(),
            dtype = {'geometry': 'GEOMETRY(Point, 4326)'}
        )

        # นับจำนวนแถวที่บัทึกลงฐานข้อมูล
        rows_loaded = len(processed_file)


        # ------------------------------------------------

        print(f"\n{SUB_SEPARATOR}")
        print(f"--- โหลดข้อมูล ไปที่ Postgre/PostGIS ---")
        print(SUB_SEPARATOR)
        print(f"โหลดข้อมูล {rows_loaded} แถว ลงตาราง '{TABLE_NAME}' สำเร็จ")

        # ------------------------------------------------


        with engine.begin() as connection:
        # with engine.connect() as connection:
            
            # query เพื่อดูข้อมูลตัวอย่าง
            select_query = sqlalchemy.text(f"SELECT * FROM {TABLE_NAME} LIMIT 10")
            execute_select_query = connection.execute(select_query).mappings().all()

            # query เพื่อนับข้อมูล
            count_query = sqlalchemy.text(f"SELECT COUNT(*) FROM {TABLE_NAME}")
            execute_count_query = connection.execute(count_query).scalar()  #.scalar() ดึงผลลัพธ์แถวแรก

            # เลือกคอมลัมน์
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
            'net',
            'code',
            'sources',
            'types',
            'type',
            'ids',
            'time_add']

            # รวมชื่อของคอลัมน์
            column_string = ', '.join(select_column)

            # query เพื่อลบข้อมูลที่ซ้ำ
            duplicates_query = sqlalchemy.text(f"""
            WITH t_dup AS (
                SELECT code,
                    ROW_NUMBER() OVER(PARTITION BY {column_string} ORDER BY time_add DESC ) AS row_num
                FROM {TABLE_NAME}
            )
            DELETE FROM {TABLE_NAME}
            WHERE code NOT IN (
                SELECT code FROM t_dup 
                WHERE row_num = 1
            );
            """)
            execute_duplicates = connection.execute(duplicates_query)
            
            # จำนวนแถวที่ถูกลบ
            rows_deleted = execute_duplicates.rowcount

            after_delete_query = sqlalchemy.text(f"SELECT COUNT(*) FROM {TABLE_NAME}")
            count_after = connection.execute(after_delete_query).scalar() #.scalar() ดึงผลลัพธ์แถวแรก

        
        # ------------------------------------------------

        print(f"\n{SUB_SEPARATOR}")
        print(f"--- สรุปข้อมูลการโหลด ---")
        print(SUB_SEPARATOR)
        print(f" - จำนวนแถวที่โหลดใหม่เข้าตาราง: {rows_loaded} แถว")
        print(f" - จำนวนแถวทั้งหมดก่อนลบข้อมูลซ้ำ: {execute_count_query} แถว")
        print(f" - จำนวนแถวที่ถูกลบออกไป (รายการซ้ำ): {rows_deleted} แถว")
        print(f" - จำนวนแถวคงเหลือในตาราง '{TABLE_NAME}' (Unique Rows): {count_after} แถว")
        print(f"--- ตัวอย่างข้อมูล ---")
        print(f"{execute_select_query}")
        print(f"Geospatial ETL Pipeline เสร็จสิ้น")

        # ------------------------------------------------

    except Exception as e:
        print(f"เกิดข้อผิดพลาดที่ def load_geojson(database_url, **kwargs): {e}")
        raise