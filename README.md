# 🌍 Project Earthquakes Data Pipeline

โปรเจกต์นี้คือการจำลองระบบ Data Pipeline แบบ End-to-End โดยใช้ Apache Airflow เพื่อดึงข้อมูลเหตุการณ์แผ่นดินไหวล่าสุดจาก Public API, ประมวลผล และจัดเก็บในฐานข้อมูลเชิงพื้นที่ (Spatial Database) เพื่อพร้อมนำไปแสดงผลผ่าน API

---

## 🛠️ เครื่องมือและเทคโนโลยี (Tech Stack)

| ประเภท | เครื่องมือ | บทบาทในโปรเจกต์ |
| :--- | :--- | :--- |
| **Orchestration** | **Apache Airflow** | จัดการ Workflow ETL/ELT และกำหนดตารางเวลาการรัน (Scheduler) |
| **Containerization** | **Docker / Docker Compose** | สร้างและจัดการสภาพแวดล้อมทั้งหมด (Airflow, Databases, API) |
| **Database** | **PostgreSQL / PostGIS** | เป็น Metadata Database ของ Airflow และเป็น Data Warehouse สำหรับเก็บข้อมูลแผ่นดินไหว (PostGIS ใช้สำหรับการวิเคราะห์เชิงพื้นที่) |
| **ETL/API Logic** | **Python** | ใช้ในการเขียน Logic สำหรับการดึงข้อมูล, แปลงข้อมูล (ETL Scripts) และสร้าง API Server (FastAPI) |
| **API Framework** | **FastAPI** | สร้าง RESTful API สำหรับแสดงผลข้อมูลสรุปแผ่นดินไหวรายชั่วโมง |

---

## 📦 แหล่งข้อมูล (Data Sources)

* **Public API:** (ระบุชื่อ API ถ้าทราบ เช่น **USGS Earthquake API**)

---

## 📈 สถาปัตยกรรมและ Workflow Diagram

```mermaid
flowchart TD
    subgraph S[Source]
        A[Public API (USGS, etc.)]
    end

    subgraph ETL[ETL Pipeline (Managed by Airflow)]
        B[Task 1: Extract Data] --> C[Task 2: Transform / Clean & Geo-Process]
        C --> D[Task 3: Load Data to PostGIS]
    end

    subgraph DW[Data Warehouse & Serving Layer]
        P[PostgreSQL / PostGIS Tables]
    end
    
    subgraph API[Serving API (FastAPI)]
        E[FastAPI Application] --> F{Query Summary Data By Hour}
        F --> G[Public API Endpoint]
    end

    A --> B
    D --> P
    P --> F
    G
