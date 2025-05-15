import os
import pandas as pd

input_folder = '.'  # โฟลเดอร์ที่ไฟล์อยู่
output_file = './weather_data_2025_05_13.xlsx'

# หาไฟล์ .parquet จาก subfolders
parquet_files = []
for root, dirs, files in os.walk(input_folder):
    for file in files:
        if file.endswith('.parquet'):
            parquet_files.append(os.path.join(root, file))

print(f"📦 พบ {len(parquet_files)} ไฟล์")

if not parquet_files:
    print("⚠️ ไม่พบไฟล์ .parquet")
    exit(1)

# โหลดไฟล์ .parquet และรวม
dfs = [pd.read_parquet(f) for f in parquet_files]

# ลบโซนเวลา (timezone) ในคอลัมน์ datetime
for df in dfs:
    for col in df.select_dtypes(include=['datetime']):
        # ลบโซนเวลา
        if df[col].dt.tz is not None:
            df[col] = df[col].dt.tz_localize(None)
        # เช็คว่า datetime ยังมีโซนเวลาหรือไม่
        if df[col].dt.tz is not None:
            print(f"⚠️ คอลัมน์ {col} ยังมีโซนเวลา")
        else:
            print(f"✅ คอลัมน์ {col} ไม่มีโซนเวลา")

# รวมข้อมูลทั้งหมด
combined_df = pd.concat(dfs, ignore_index=True)

# ลบโซนเวลา (timezone) ทุกรูปแบบในคอลัมน์ datetime
for col in combined_df.select_dtypes(include=['datetime']):
    if combined_df[col].dt.tz is not None:
        combined_df[col] = combined_df[col].dt.tz_localize(None)

# 🔁 ลบ timezone อีกรอบใน combined_df (กรณียังหลงเหลือ)
for col in combined_df.columns:
    if pd.api.types.is_datetime64_any_dtype(combined_df[col]):
        if hasattr(combined_df[col].dt, 'tz') and combined_df[col].dt.tz is not None:
            combined_df[col] = combined_df[col].dt.tz_localize(None)

# แปลงเป็นไฟล์ Excel
combined_df.to_excel(output_file, index=False, engine='openpyxl')

print(f"✅ แปลงสำเร็จ: {output_file}")