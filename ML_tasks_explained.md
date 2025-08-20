# Machine Learning Tasks in Luigi

This document explains how the ML pipeline is implemented inside `ml_tasks.py` and how it integrates with the Luigi workflow.

---

## 🎯 Purpose
The ML tasks extend the ETL pipeline by:
1. Generating **product recommendations** using purchase history.
2. Creating **sales forecasts** using time series modeling.

Both outputs are written back to MySQL for analysis and reporting.

---

## 📂 Task Breakdown

### 1. `TrainRecommendations`
- **Input**: `order_items`, `products` tables from MySQL.
- **Process**:
  - Pivot order history into a product–product co-occurrence matrix.
  - Apply **cosine similarity** to compute item-to-item affinity.
  - Rank recommendations per product.
- **Output**: Writes to `product_recommendations` table with columns:
  - `product_id`
  - `recommended_product_id`
  - `score`
  - `rank`

⚠️ **Duplicate Handling**: The pipeline ensures uniqueness per `(product_id, recommended_product_id)` by grouping and ranking. If you still see duplicates, check pivoting logic or missing DISTINCT clauses.

---

### 2. `TrainForecasting`
- **Input**: `orders` table (aggregated by date).
- **Process**:
  - Prepares daily sales time series.
  - Trains forecasting model (default: Prophet or ARIMA).
  - Predicts sales for the next N days.
- **Output**: Writes to `sales_forecast` table with columns:
  - `forecast_date`
  - `predicted_sales`
  - `lower_ci`
  - `upper_ci`

---

## ⚙️ Luigi Integration
Each ML task subclasses `luigi.Task` and defines:
- `requires()` → ensures ETL tasks finish before ML runs.
- `output()` → marks completion via database table + Luigi marker.
- `run()` → executes Python ML logic (scikit-learn, pandas, Prophet).

---

## 📊 Data Flow
```
Raw CSVs (/data) → ETL (etl_luigi.py) → MySQL tables → ML tasks (ml_tasks.py) → MySQL (recommendations, forecasts)
```

---

## 🛠 Common Issues
1. **Duplicates in recommendations** → caused by symmetry (A→B, B→A). Use filtering + ranking to avoid exact duplicates.
2. **Empty forecasts** → check if orders table has enough history (>30 days recommended).
3. **Scheduler not showing tasks** → ensure Luigi marker files are cleared when DB is reset:
   ```bash
   rm -f /app/.luigi_work/*.loaded
   ```

---

## 🔮 Next Steps
- Extend recommendations with collaborative filtering (ALS, matrix factorization).
- Enhance forecasting with external regressors (e.g., seasonality, promotions).
- Add **model evaluation tasks** to validate performance.

---

This file is intended for contributors who want to understand **why** and **how** ML is included in the Luigi pipeline.


---

## 🧪 SQL Cheat Sheet (verify outputs)

### General
```sql
-- list tables
SHOW TABLES;

-- row counts
SELECT 'orders' AS t, COUNT(*) c FROM orders
UNION ALL SELECT 'order_items', COUNT(*) FROM order_items
UNION ALL SELECT 'payments', COUNT(*) FROM payments
UNION ALL SELECT 'products', COUNT(*) FROM products
UNION ALL SELECT 'product_recommendations', COUNT(*) FROM product_recommendations
UNION ALL SELECT 'sales_forecast_daily', COUNT(*) FROM sales_forecast_daily;
```

### Recommendations
```sql
-- Top-N recommendations for a given product
SELECT *
FROM product_recommendations
WHERE product_id = 'PROD001'
ORDER BY rank ASC
LIMIT 10;

-- What are the most frequently co-bought items with a given product?
-- (requires `co_buyers` column from ml_tasks.py)
SELECT recommended_product_id, co_buyers
FROM product_recommendations
WHERE product_id = 'PROD001'
ORDER BY co_buyers DESC, rank ASC
LIMIT 10;

-- Symmetric check: do we also recommend A from B?
SELECT a.product_id, a.recommended_product_id AS rec_from_a,
       b.product_id AS back_from_rec, b.recommended_product_id AS rec_from_b
FROM product_recommendations a
LEFT JOIN product_recommendations b
  ON a.product_id = b.recommended_product_id
 AND a.recommended_product_id = b.product_id
WHERE a.product_id = 'PROD001';

-- (Optional) collapse symmetric pairs into one row (analysis view)
SELECT LEAST(product_id, recommended_product_id)  AS prod_a,
       GREATEST(product_id, recommended_product_id) AS prod_b,
       MAX(score) AS max_score, MAX(co_buyers) AS max_cobuyers
FROM product_recommendations
GROUP BY prod_a, prod_b
ORDER BY max_score DESC
LIMIT 20;

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_prod_recs_pid ON product_recommendations(product_id);
CREATE INDEX IF NOT EXISTS idx_prod_recs_rec ON product_recommendations(recommended_product_id);
```

### Forecasts
```sql
-- Latest 14 days of forecast
SELECT *
FROM sales_forecast_daily
ORDER BY ds DESC
LIMIT 14;

-- Weekly view of the forecast
SELECT DATE_FORMAT(ds, '%Y-%u') AS year_week,
       AVG(yhat) AS avg_pred,
       AVG(yhat_lower) AS avg_lo,
       AVG(yhat_upper) AS avg_hi
FROM sales_forecast_daily
GROUP BY year_week
ORDER BY year_week DESC
LIMIT 8;

-- Join actuals vs forecast (if you have daily actuals from payments)
-- Build a daily actuals CTE
WITH daily_actuals AS (
  SELECT DATE(o.order_date) AS ds, SUM(p.payment_value) AS y_actual
  FROM orders o
  JOIN payments p ON o.order_id = p.order_id
  GROUP BY DATE(o.order_date)
)
SELECT f.ds, f.yhat, f.yhat_lower, f.yhat_upper, a.y_actual
FROM sales_forecast_daily f
LEFT JOIN daily_actuals a USING (ds)
ORDER BY f.ds DESC
LIMIT 30;
```

### Export results (if needed)
- **Workbench/DBeaver**: right–click table → *Export result set* → CSV.
- **Pandas (from your host)**:
```python
import pandas as pd, sqlalchemy as sa
engine = sa.create_engine("mysql+pymysql://root:<PW>@127.0.0.1:3306/ecommerce")
recs = pd.read_sql("SELECT * FROM product_recommendations", engine)
recs.to_csv("product_recommendations.csv", index=False)
fc = pd.read_sql("SELECT * FROM sales_forecast_daily", engine)
fc.to_csv("sales_forecast_daily.csv", index=False)
```
