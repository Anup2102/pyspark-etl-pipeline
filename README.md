
# NYC Taxi Data PySpark ETL Pipeline (Freelance Portfolio)

## 🚀 Overview

This project demonstrates a **reusable, automated ETL (Extract, Transform, Load) pipeline using PySpark and Python** for structured data processing.

The pipeline:
- Reads raw NYC taxi data from CSV.
- Cleans and validates records.
- Adds a `load_date` column for data lineage.
- Writes partitioned Parquet files for efficient analytics and reporting.

This is part of my **Upwork data engineering portfolio** to showcase practical PySpark ETL skills for scalable data pipelines.

---

## 🛠️ Features

✅ Read structured CSV data using PySpark  
✅ Data cleaning and validation (removing invalid trips, missing data)  
✅ Dynamic `load_date` injection for each ETL run  
✅ Write output as partitioned Parquet files for efficient querying  
✅ Modular, clean, and reusable ETL code structure

---

## ⚙️ Tech Stack

- Python 3.10+
- PySpark 3.x
- Pandas (optional, for output validation)
- Git / GitHub

---

## 📂 Folder Structure


.
├── main_etl.py
├── nyc_taxi_sample.csv
├── requirements.txt
└── output_parquet/   # Created after ETL run


---

## 🗂️ Dataset

Uses a **sample NYC taxi dataset** (`nyc_taxi_sample.csv`) with fields:
- `trip_id`
- `pickup_datetime`
- `dropoff_datetime`
- `passenger_count`
- `trip_distance`
- `fare_amount`

---

## 🚀 How to Run


1️⃣ **Clone the repository:**
```bash
git clone https://github.com/Anup2102/pyspark-etl-pipeline.git
cd pyspark-etl-pipeline
```

2️⃣ **Install dependencies:**
```bash
pip install -r requirements.txt
```

3️⃣ **Run the ETL pipeline:**
```bash
python main_etl.py
```

Check the `output_parquet/` directory for the clean, partitioned Parquet files.

### 🎯 Use Cases
- Demonstrating PySpark ETL skills for Upwork projects
- Testing PySpark setup locally on Windows/Mac/Linux
- Building a scalable ETL base for freelance or enterprise pipelines
- Practicing partitioned data writes and validation

### 📈 Future Enhancements
- Modularize transformation logic for reusability
- Add schema enforcement using StructType
- Integrate cloud storage output (AWS S3/Azure Blob) for pipeline extension
- Include data quality checks and logging
- Add unit tests for pipeline stages

---

📞 **Contact**  
Anup Singh  
Data Engineer | PySpark Specialist | Open to Freelance Projects
