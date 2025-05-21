import os
import pandas as pd

# 🔁 เปลี่ยน input_folder ให้เป็นโฟลเดอร์ระดับเดือน
input_folder = '../2025-05/'  # หรือ path ที่เก็บไฟล์ .parquet ทั้งเดือน
output_file = './weather_data_2025_05.xlsx'

# หาไฟล์ .parquet จาก subfolders (เช่น day=13/hour=.../xxx.parquet)
parquet_files = []
for root, dirs, files in os.walk(input_folder):
    for file in files:
        if file.endswith('.parquet'):
            parquet_files.append(os.path.join(root, file))

print(f"📦 พบ {len(parquet_files)} ไฟล์")

if not parquet_files:
    print("⚠️ ไม่พบไฟล์ .parquet")
    exit(1)

# โหลดทุกไฟล์
dfs = [pd.read_parquet(f) for f in parquet_files]

# ลบ timezone ในทุกคอลัมน์ datetime ของแต่ละ DataFrame
for df in dfs:
    for col in df.select_dtypes(include=['datetime']):
        if df[col].dt.tz is not None:
            df[col] = df[col].dt.tz_localize(None)

# รวมเป็น DataFrame เดียว
combined_df = pd.concat(dfs, ignore_index=True)

# ลบ timezone อีกครั้งหลังรวม (เพื่อความแน่ใจ)
for col in combined_df.columns:
    if pd.api.types.is_datetime64_any_dtype(combined_df[col]):
        if hasattr(combined_df[col].dt, 'tz') and combined_df[col].dt.tz is not None:
            combined_df[col] = combined_df[col].dt.tz_localize(None)

# เขียนลง Excel
combined_df.to_excel(output_file, index=False, engine='openpyxl')
print(f"✅ แปลงสำเร็จ: {output_file}")