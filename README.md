# Luigi E-Commerce ETL with MySQL (Dockerized)

This project builds an **ETL pipeline** using [Luigi (Spotify)](https://github.com/spotify/luigi) to process e-commerce CSV datasets and load them into a MySQL database. Everything you need—MySQL, Luigi, Python dependencies, and the ETL pipeline—is packaged into **one Docker image**.

---

## 📂 Datasets

Sample CSV datasets are provided for **testing the pipeline only**:

- `customers.csv`  
- `order_items.csv`  
- `orders.csv`  
- `payments.csv`  
- `products.csv`

👉 **Important:**  
Do **not** commit your dataset folder (`/datasets`) into Git. It should remain local and is mounted into the container at runtime for testing and development. Production data sources should be configured separately.

By default, datasets are expected under:  
`/Users/nina/Downloads/datasets`

They will be mounted into the container at `/data`.

---

## ⚙️ Pipeline Flow

1. **Extract** – Luigi reads CSV files from `/data`.  
2. **Transform** – Column names are validated and normalized:
   - lowercased, trimmed, spaces/hyphens → underscores  
   - special characters removed  
   - duplicate names checked (fails if duplicates after normalization)  
3. **Load** – Transformed data is loaded into MySQL (`ecommerce` DB) with tables named after the CSV files.  
   - Example: `customers.csv` → `customers` table  
   - Load mode can be `replace` (default) or `append`.

---

## 🐳 Docker Setup

### 1. Build the image
```
docker build -t nina/luigi-mysql:latest .
```
### 2. Run the container
```
docker run -d --name luigi-etl \
  -e MYSQL_ROOT_PASSWORD="your_strong_pw" \
  -p 3306:3306 -p 8082:8082 \
  -v /Users/nina/Downloads/datasets:/data \
  nina/luigi-mysql:latest
```
- MySQL available at localhost:3306
- Luigi Scheduler UI at http://localhost:8082

⚠️ Passwords with special characters like @ must be URL-encoded (e.g., Nin%40cut3) unless you’ve patched the code to URL-encode credentials.

### 3. Run the ETL job
```
docker exec -it luigi-etl bash -lc '
export PYTHONPATH=/app
python3 -m luigi --module etl_luigi LoadAll \
  --local-scheduler \
  --ETLConfig-data-dir /data \
  --ETLConfig-db-host 127.0.0.1 \
  --ETLConfig-db-port 3306 \
  --ETLConfig-db-user root \
  --ETLConfig-db-password "$MYSQL_ROOT_PASSWORD" \
  --ETLConfig-db-name ecommerce \
  --ETLConfig-load-mode replace'
``` 
📊 Outputs
- MySQL DB: ecommerce
- Tables: customers, order_items, orders, payments, products
- Audit Logs: Column normalization logs written to .luigi_work/*.log
- Intermediate Data: Parquet files stored under .luigi_work/

🛠 Configuration
Defaults are stored in luigi.cfg:

```[ETLConfig]
data_dir=/data
db_host=127.0.0.1
db_port=3306
db_user=root
db_password=changeme
db_name=ecommerce
load_mode=replace
```
Override at runtime using Luigi CLI flags.

✅ Features
- Single Docker image (MySQL + Luigi + ETL)
- Automatic DB creation (init.sql)
- Column normalization & duplicate checking
- Extensible: add new datasets or schema checks easily
- Includes pyarrow (for Parquet) and cryptography (for MySQL 8 auth)

🚫 What Not to Commit
- .idea/ (PyCharm/IntelliJ configs)
- .luigi_work/ (Luigi intermediate files)
- __pycache__/, .venv/ (Python cache/envs)
- datasets/ folder (used only for local testing)

All of these are covered in the included `.gitignore.`

🔍 How to Inspect MySQL
CLI inside container
`docker exec -it luigi-etl mysql -uroot -p`
From host
`mysql -h 127.0.0.1 -P 3306 -uroot -p`
Then:
```
SHOW DATABASES;
USE ecommerce;
SHOW TABLES;
SELECT COUNT(*) FROM customers;
```
Or connect with a GUI (MySQL Workbench, DBeaver, TablePlus).

🚀 Next Steps
- Add schema validation rules per table
- Define primary keys & indexes after load
- Automate ETL run on container startup (via Supervisor healthcheck)
