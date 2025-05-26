# ระบบเก็บและวิเคราะห์ข้อมูลสภาพอากาศแบบอัตโนมัติด้วย Prefect, LakeFS, และ Streamlit  
### รายวิชา DSI321: โครงสร้างพื้นฐานคอมพิวเตอร์สำหรับการประมวลผลข้อมูลขนาดใหญ่

## 📌 คำอธิบายโครงการ

โครงการนี้จัดทำขึ้นเพื่อพัฒนา Data Pipeline ที่สามารถเก็บข้อมูลสภาพอากาศแบบ real-time จาก API ของ OpenWeatherMap โดยมีสถานที่เป้าหมายคือ **สมาคมศิษย์เก่าโรงเรียนสาธิตรามคำแหง และพื้นที่ใกล้เคียงอีก 14 แห่ง รวมทั้งสิ้น 15 แห่ง** ในทุก ๆ 15 นาที ข้อมูลที่ได้จะถูกจัดเก็บแบบ version-controlled บน LakeFS และนำมาวิเคราะห์และแสดงผลผ่าน Dashboard บน Streamlit พร้อมเสริมด้วย Machine Learning เพื่อให้ข้อมูลมีบริบทที่เข้าใจง่าย

## 🎯 วัตถุประสงค์

- ทดลองใช้ Prefect ในการจัดการ Workflow จริง
- สร้างระบบที่สามารถเก็บข้อมูลจำนวนมากอย่างมีโครงสร้าง
- พัฒนา Dashboard ที่ช่วยให้ผู้ใช้งานเข้าใจข้อมูลสภาพอากาศได้แบบ Interactive
- นำ Machine Learning มาช่วยจำแนกและสรุปลักษณะของข้อมูล
- ปูพื้นฐานการใช้ Docker และ LakeFS สำหรับระบบ Data Engineering

## 🧱 เทคโนโลยีที่ใช้

| เทคโนโลยี | จุดประสงค์ |
|-----------|-------------|
| Docker | รันทุก component ใน environment เดียว |
| Prefect | ควบคุมการดึงข้อมูลแบบ workflow และทำ schedule |
| LakeFS | จัดเก็บข้อมูลแบบ version control ในรูปแบบ Parquet |
| Streamlit | สร้าง dashboard แสดงผลข้อมูลแบบ interactive |
| OpenWeatherMap API | ดึงข้อมูลสภาพอากาศแบบ real-time |
| Python (Pandas, Plotly) | วิเคราะห์และจัดรูปแบบข้อมูล |
| K-Means Clustering | จำแนกลักษณะสภาพอากาศ |


## 🗺️ ระบบทำงานอย่างไร?

1. **Prefect agent** จะทำงานอยู่ใน Docker และทำการดึงข้อมูลจาก OpenWeatherMap API ทุก ๆ 15 นาที
2. ข้อมูลที่ดึงมา จะถูกทำความสะอาด แปลง schema และจัดเก็บใน LakeFS ในรูปแบบไฟล์ `.parquet`
3. Streamlit จะดึงข้อมูลจาก LakeFS มาแสดงผลแบบ real-time และย้อนหลังผ่าน dashboard
4. นำข้อมูลที่สะสมมาวิเคราะห์ด้วย K-Means เพื่อจำแนกลักษณะอากาศ เช่น ฝนตก อากาศร้อน อากาศเย็น
5. ผู้ใช้สามารถดูข้อมูลย้อนหลังแบบกราฟ และดูคำแนะนำเชิงบริบทจากกลุ่มที่ระบบจัดให้

## 📊 Data Schema ที่จัดเก็บ

| Field | Description |
|-------|-------------|
| timestamp | เวลาที่ดึงข้อมูล |
| location | พิกัดละติจูด-ลองจิจูด |
| province | จังหวัด |
| site_name | ชื่อสถานที่ |
| temperature | อุณหภูมิ (°C) |
| humidity | ความชื้น (%) |
| wind_speed | ความเร็วลม |
| rain | ปริมาณฝน |
| weather_main | สภาพอากาศ (เช่น Clear, Rain, Cloudy) |

> 🔢 **ข้อมูลที่จัดเก็บแล้วปัจจุบัน**: ~10,000 แถว (อัปเดตจำนวนจริงตามล่าสุด)

---

## 🔁 การจัดการ Workflow ด้วย Prefect

Prefect ถูกตั้งเวลาให้ทำงานทุก 15 นาที (ใช้ `IntervalSchedule`) โดยแต่ละ Flow ประกอบด้วยขั้นตอน:
1. Fetch ข้อมูลจาก API ตามพิกัดของทั้ง 15 สถานที่
2. ตรวจสอบรูปแบบข้อมูล
3. สร้างไฟล์ Parquet
4. เขียนลง LakeFS branch
5. ตรวจสอบความสำเร็จของ task (มี retry หากล้มเหลว)

> 🖼️ **[ใส่ภาพ Prefect Flow ได้ที่นี่]**  
> `![Prefect Flow](./images/prefect-flow.png)`

---

## 🧊 การจัดเก็บข้อมูลด้วย LakeFS

LakeFS ช่วยให้สามารถ:
- สร้าง branch เพื่อเก็บข้อมูลทดลอง
- ทำ commit version ของ dataset ได้
- rollback หากพบปัญหา

ทุกข้อมูลจัดเก็บในรูปแบบ **Parquet** ซึ่งมีข้อดีคือ:
- บีบอัดได้ดี
- อ่านเร็วเฉพาะ column ที่ต้องการ
- รองรับงาน ML และ Data Analysis ได้ดี

> 🖼️ **[ใส่ภาพหน้า LakeFS]**  
> `![LakeFS Parquet](./images/lakefs-data.png)`

---

## 📈 การแสดงผลด้วย Streamlit

สร้าง dashboard ที่สามารถ:
- แสดงข้อมูลตามสถานที่ จังหวัด หรือช่วงเวลา
- มีกราฟเส้นแนวโน้ม (temperature/humidity/rain)
- แสดงข้อมูลย้อนหลัง 7 วัน
- ดู Raw Data และ Export CSV ได้

> 🖼️ **[ใส่ภาพ Streamlit Dashboard 1-4]**  
> `![Dashboard Overview](./images/dashboard-overview.png)`  
> `![Dashboard Filters](./images/dashboard-filter.png)`  
> `![Raw Data Table](./images/dashboard-rawdata.png)`  
> `![Rainfall Graph](./images/dashboard-rain.png)`

---

## 🤖 การวิเคราะห์ด้วย K-Means Clustering

ข้อมูลถูกนำเข้า clustering model เพื่อจัดกลุ่ม เช่น:
- กลุ่ม 1: อากาศร้อนชื้น
- กลุ่ม 2: ฝนตกสม่ำเสมอ
- กลุ่ม 3: อากาศเย็นแห้ง

> ผลลัพธ์แต่ละกลุ่มจะมีคำอธิบาย เช่น:
> - กลุ่ม 1: "ควรสวมเสื้อผ้าเบา ๆ ดื่มน้ำมาก"
> - กลุ่ม 3: "อุณหภูมิต่ำ ควรเตรียมเสื้อกันหนาว"

> 🖼️ **[ใส่ภาพ Clustering Chart]**  
> `![KMeans Result](./images/kmeans-chart.png)`

---

## 👨‍💻 ข้อมูลผู้จัดทำ

- ชื่อ-นามสกุล: (ใส่ชื่อจริง)
- รหัสนักศึกษา: (ใส่รหัสนักศึกษา)
- ภาควิชา: วิทยาการข้อมูลและนวัตกรรม (DSI)
- สังกัด: มหาวิทยาลัยรามคำแหง

---

## 📝 วิธีเรียกใช้งานเบื้องต้น (Optional)

```bash
# สร้าง image และรัน Docker container
docker compose up --build

# เข้าหน้า dashboard
localhost:8501 (Streamlit)
