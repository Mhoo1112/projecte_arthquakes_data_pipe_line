import pandas
import geopandas
import json
import requests
import os
import datetime
import pytz
import time

# จำลองข้อมูลที่ได้จากฐานข้อมูล (List of Dictionaries)
mock_db_data = [
    {
        'hour_of_day': datetime.datetime(2025, 10, 28, 0, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=7))),
        'mag_total': 5, 'mag_000_to_099': 2, 'mag_100_to_199': 2, 'mag_200_to_299': 1,
        'mag_300_to_399': 0, 'mag_400_to_499': 0, 'mag_500_to_599': 0,
        'mag_600_to_699': 0, 'mag_700_to_799': 0, 'mag_800_to_899': 0, 'mag_900_to_999': 0
    },
    {
        'hour_of_day': datetime.datetime(2025, 10, 28, 1, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=7))),
        'mag_total': 8, 'mag_000_to_099': 4, 'mag_100_to_199': 3, 'mag_200_to_299': 1,
        'mag_300_to_399': 0, 'mag_400_to_499': 0, 'mag_500_to_599': 0,
        'mag_600_to_699': 0, 'mag_700_to_799': 0, 'mag_800_to_899': 0, 'mag_900_to_999': 0
    },
    # ... (ข้อมูลชั่วโมงอื่นๆ ตามตารางข้างบน)
]

# การสร้าง DataFrame (ถ้าคุณเลือกที่จะใช้ Pandas ในการประมวลผลต่อ)
df = pandas.DataFrame(mock_db_data)

# พิมพ์ตัวอย่าง DataFrame
print(df.head())

for row in df:
    print(row)