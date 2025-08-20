# Luigi + MySQL + Machine Learning ETL Pipeline (Dockerized)

This project demonstrates how to orchestrate a complete ETL + ML workflow using **Luigi** inside Docker. It extracts e-commerce data, loads it into **MySQL**, trains a recommendation system and a forecasting model, and writes results back to the database.


## 📂 Project Structure
```
Dockerized_Luigi_Pipeline/
├── Dockerfile # Docker build definition
├── etl_luigi.py # ETL pipeline tasks (extract, load, transform)
├── ml_tasks.py # ML tasks (recommendations + forecasting)
├── requirements.txt # Python dependencies
├── README.md # Main documentation
└── ml_tasks_explained.md # Extra: detailed notes about ml_tasks.py design
```

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

## 🚀 Quickstart
### 4. Run Luigi ETL
```bash
export PYTHONPATH=/app
python3 -m luigi --module etl_luigi LoadOrders \
--scheduler-host 127.0.0.1 --scheduler-port 8082 \
--ETLConfig-data-dir /data \
--ETLConfig-db-host 127.0.0.1 \
--ETLConfig-db-port 3306 \
--ETLConfig-db-user root \
--ETLConfig-db-password "$MYSQL_ROOT_PASSWORD" \
--ETLConfig-db-name ecommerce
```


### 5. Run ML tasks
#### Recommendations
```bash
python3 -m luigi --module ml_tasks TrainRecommendations \
--scheduler-host 127.0.0.1 --scheduler-port 8082 \
--ETLConfig-data-dir /data \
--ETLConfig-db-host 127.0.0.1 \
--ETLConfig-db-port 3306 \
--ETLConfig-db-user root \
--ETLConfig-db-password "$MYSQL_ROOT_PASSWORD" \
--ETLConfig-db-name ecommerce
```


#### Forecasting
```bash
python3 -m luigi --module ml_tasks TrainForecasting \
--scheduler-host 127.0.0.1 --scheduler-port 8082 \
--ETLConfig-data-dir /data \
--ETLConfig-db-host 127.0.0.1 \
--ETLConfig-db-port 3306 \
--ETLConfig-db-user root \
--ETLConfig-db-password "$MYSQL_ROOT_PASSWORD" \
--ETLConfig-db-name ecommerce
```


---


## 🗄 Database Tables
- **orders** – raw orders
- **order_items** – order line items
- **products** – product catalog
- **product_recommendations** – ML-generated item–item recommendations
- **sales_forecast** – ML-generated future sales predictions


---


## 📊 Luigi Visualizer
Access task execution graph at:
```
http://localhost:8082/static/visualiser/index.html
```


---


## 🧠 ML Details
- **Recommendations** – Item-to-item cosine similarity based on co-purchases. Includes fallback to popularity when strict filters yield no results.
- **Forecasting** – Time series model (Prophet/ARIMA) trained on order history to predict future sales.


---


## ⚠️ Notes
- By default, Luigi markers (".loaded") are stored in `/app/.luigi_work/`.
- If you drop/reload the DB, clear markers:
```bash
rm -f /app/.luigi_work/*.loaded
```
- Always rebuild the image after changing `Dockerfile`, `etl_luigi.py`, or `ml_tasks.py`.


---

## 📖 Further Reading
See [ml_tasks_explained.md](ml_tasks_explained.md) for an in-depth explanation of the machine learning pipeline logic.

## 🚫 What Not to Commit

* `.idea/` (PyCharm/IntelliJ configs)
* `.luigi_work/` (Luigi intermediate files)
* `__pycache__/`, `.venv/` (Python cache/envs)
* `datasets/` folder (used only for local testing)

All of these are covered in the included `.gitignore`.

---

## 🔎 Quick SQL checks

After running the pipeline you can verify outputs directly in MySQL:

```sql
-- show tables
SHOW TABLES;

-- peek recommendations for a product
SELECT *
FROM product_recommendations
WHERE product_id = 'PROD001'
ORDER BY rank
LIMIT 10;

-- peek latest forecast
SELECT *
FROM sales_forecast_daily
ORDER BY ds DESC
LIMIT 14;
```
For a larger “SQL cheat sheet” (indexes, pair collapsing, actuals vs forecast joins), see ml_tasks_explained.md.
