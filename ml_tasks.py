import os
from typing import Optional
import luigi
import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from statsmodels.tsa.statespace.sarimax import SARIMAX
from etl_luigi import ETLConfig, make_engine, LoadAll

# ---------------------------
# Recommendations (co-purchase)
# ---------------------------
class TrainRecommendations(luigi.Task):
    """
    Item-item recommendations from co-purchases using cosine similarity.
    Writes to MySQL table: product_recommendations(product_id, recommended_product_id, score, rank, co_buyers)
    """
    top_n = luigi.IntParameter(default=5)
    min_copurchase = luigi.IntParameter(default=1)          # min customers who bought an item
    min_pair_copurchase = luigi.IntParameter(default=1)     # min customers who bought BOTH items
    drop_zero = luigi.BoolParameter(default=True)           # don't keep 0.0 similarity
    dedupe_pairs = luigi.BoolParameter(default=False)       # if True, keep one row per unordered pair

    def requires(self):
        return LoadAll()

    def output(self):
        return luigi.LocalTarget(os.path.join(".luigi_work", "product_recommendations.loaded"))

    def run(self):
        cfg = ETLConfig()
        engine = make_engine(cfg.db_host, cfg.db_port, cfg.db_user, cfg.db_password, cfg.db_name)

        sql = """
              SELECT o.customer_id, oi.product_id, COUNT(*) AS purchase_count
              FROM orders o
                       JOIN order_items oi ON o.order_id = oi.order_id
              GROUP BY o.customer_id, oi.product_id \
              """
        df = pd.read_sql(sql, engine)
        if df.empty:
            raise ValueError("No data for recommendations.")

        # Normalize IDs
        df["customer_id"] = df["customer_id"].astype(str).str.strip()
        df["product_id"] = df["product_id"].astype(str).str.strip().str.upper()

        # User–item matrix
        mat = df.pivot(index="customer_id", columns="product_id",
                       values="purchase_count").fillna(0)

        # Filter rare items
        item_counts = (mat > 0).sum(axis=0)
        mat = mat.loc[:, item_counts[item_counts >= self.min_copurchase].index]
        if mat.shape[1] < 2:
            raise ValueError("Not enough co-purchased products.")

        # Similarity + true co-occurrence counts
        bin_mat = (mat > 0).astype(int)
        co_counts = bin_mat.T @ bin_mat  # customers who bought both
        sim = cosine_similarity(mat.T)

        products = mat.columns.tolist()
        sim_df = pd.DataFrame(sim, index=products, columns=products)
        co_df = pd.DataFrame(co_counts.values, index=products, columns=products)

        rows = []
        for pid in products:
            scores = sim_df[pid].drop(pid)
            valid = co_df[pid].drop(pid) >= self.min_pair_copurchase
            scores = scores[valid]
            if self.drop_zero:
                scores = scores[scores > 0]

            top = scores.sort_values(ascending=False).head(self.top_n)

            # If strict filters produced nothing, fallback to co-purchase counts
            if top.empty:
                fallback = co_df[pid].drop(pid)
                fallback = fallback[fallback > 0].sort_values(ascending=False).head(self.top_n)
                for rank, (rec_pid, cnt) in enumerate(fallback.items(), start=1):
                    rows.append({
                        "product_id": pid,
                        "recommended_product_id": rec_pid,
                        "score": 0.0,  # popularity fallback
                        "rank": int(rank),
                        "co_buyers": int(cnt)
                    })
            else:
                for rank, (rec_pid, score) in enumerate(top.items(), start=1):
                    rows.append({
                        "product_id": pid,
                        "recommended_product_id": rec_pid,
                        "score": float(score),
                        "rank": int(rank),
                        "co_buyers": int(co_df.at[rec_pid, pid])
                    })

        # Ensure schema exists even if rows == []
        out_cols = ["product_id", "recommended_product_id", "score", "rank", "co_buyers"]
        out = pd.DataFrame(rows, columns=out_cols)

        out.to_sql("product_recommendations", con=engine, if_exists="replace",
                   index=False, method="multi", chunksize=10_000)

        with self.output().open("w") as f:
            f.write("loaded")

# ---------------------------
# Forecasting (daily→weekly fallback)
# ---------------------------
class ForecastSales(luigi.Task):
    """
    Daily sales forecasting using SARIMAX(1,1,1).
    Writes to MySQL table: sales_forecast_daily(ds, yhat, yhat_lower, yhat_upper, model)
    - Auto-detects an order date/datetime column in `orders`
    - Prefers revenue from `payments.payment_value`
      (fallback: tries order_items totals using available price/quantity columns)
    """
    horizon_days = luigi.IntParameter(default=30)

    def requires(self):
        return LoadAll()

    def output(self):
        return luigi.LocalTarget(os.path.join(".luigi_work", "sales_forecast_daily.loaded"))

    # --- helpers ---
    def _detect_order_date_column(self, engine) -> str:
        cols = pd.read_sql(
            """
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'orders'
            """,
            engine
        )
        if cols.empty:
            raise ValueError("`orders` table not found or has no columns.")
        preferred = [
            "order_date", "order_purchase_timestamp", "order_timestamp",
            "created_at", "purchase_date", "purchase_timestamp"
        ]
        candidates = cols[cols["DATA_TYPE"].str.lower().isin(["date", "datetime", "timestamp"])]
        for name in preferred:
            mask = (candidates["COLUMN_NAME"].str.lower() == name)
            if mask.any():
                return candidates.loc[mask, "COLUMN_NAME"].iloc[0]
        if not candidates.empty:
            return candidates["COLUMN_NAME"].iloc[0]
        return cols["COLUMN_NAME"].iloc[0]

    def _payments_daily_sql(self, order_date_col: str) -> str:
        return f"""
            SELECT DATE(o.`{order_date_col}`) AS ds, SUM(p.payment_value) AS y
            FROM orders o
            JOIN payments p ON o.order_id = p.order_id
            GROUP BY DATE(o.`{order_date_col}`)
            ORDER BY ds
        """

    def _order_items_daily_sql(self, order_date_col: str, engine) -> str:
        oi_cols = pd.read_sql(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'order_items'
            """,
            engine
        )["COLUMN_NAME"].str.lower().tolist()
        price_candidates = ["price", "unit_price", "item_price", "product_price", "sale_price"]
        qty_candidates   = ["quantity", "qty", "item_qty", "count"]
        price_col = next((c for c in price_candidates if c in oi_cols), None)
        qty_col   = next((c for c in qty_candidates if c in oi_cols), None)
        if price_col and qty_col:
            total_expr = f"SUM(oi.`{price_col}` * oi.`{qty_col}`)"
        elif price_col:
            total_expr = f"SUM(oi.`{price_col}`)"
        else:
            total_expr = "COUNT(*)"
        return f"""
            SELECT DATE(o.`{order_date_col}`) AS ds, {total_expr} AS y
            FROM orders o
            JOIN order_items oi ON o.order_id = oi.order_id
            GROUP BY DATE(o.`{order_date_col}`)
            ORDER BY ds
        """

    def run(self):
        cfg = ETLConfig()
        engine = make_engine(cfg.db_host, cfg.db_port, cfg.db_user, cfg.db_password, cfg.db_name)
        order_date_col = self._detect_order_date_column(engine)

        def fetch_daily():
            try:
                df = pd.read_sql(self._payments_daily_sql(order_date_col), engine)
                if df.empty:
                    raise ValueError("payments empty")
            except Exception:
                df = pd.read_sql(self._order_items_daily_sql(order_date_col, engine), engine)
            return df

        # DAILY
        df = fetch_daily()
        df["ds"] = pd.to_datetime(df["ds"])
        df = df.groupby("ds", as_index=False)["y"].sum()
        df = df.set_index("ds").asfreq("D")
        df["y"] = pd.to_numeric(df["y"], errors="coerce").fillna(0.0)

        series_used = "daily"

        # If too short, WEEKLY fallback
        if df.dropna().shape[0] < 10:
            df = df.resample("W-SUN").sum()
            df.index.name = "ds"
            series_used = "weekly"

        if df.dropna().shape[0] < 8:
            # Naive mean baseline if still too short
            hist_mean = float(df["y"].tail(5).mean()) if df.shape[0] else 0.0
            idx = pd.date_range(
                start=(df.index.max() if df.shape[0] else pd.Timestamp.today()),
                periods=int(self.horizon_days),
                freq="D"
            )
            fc = pd.DataFrame({
                "ds": idx,
                "yhat": hist_mean,
                "yhat_lower": hist_mean * 0.8,
                "yhat_upper": hist_mean * 1.2,
                "model": f"NaiveMean({series_used})"
            })
            fc.to_sql("sales_forecast_daily", con=engine, if_exists="replace", index=False, method="multi", chunksize=10_000)
            with self.output().open("w") as f:
                f.write("loaded")
            return

        # SARIMAX
        model = SARIMAX(df["y"], order=(1, 1, 1), enforce_stationarity=False, enforce_invertibility=False)
        res = model.fit(disp=False)

        steps = int(self.horizon_days) if series_used == "daily" else max(4, int(self.horizon_days // 7))
        forecast_res = res.get_forecast(steps=steps)
        pred_mean = forecast_res.predicted_mean
        conf_int = forecast_res.conf_int(alpha=0.2)

        fc = pd.DataFrame({
            "ds": pred_mean.index,
            "yhat": pred_mean.values,
            "yhat_lower": conf_int.iloc[:, 0].values,
            "yhat_upper": conf_int.iloc[:, 1].values,
            "model": f"SARIMAX(1,1,1)-{series_used}"
        })

        # If weekly model, expand to daily via forward fill for a daily-facing table
        if series_used == "weekly":
            fc = fc.set_index("ds").resample("D").ffill().reset_index()
            fc = fc.iloc[: int(self.horizon_days)]

        fc.to_sql("sales_forecast_daily", con=engine, if_exists="replace",
                  index=False, method="multi", chunksize=10_000)

        with self.output().open("w") as f:
            f.write("loaded")

# ---------------------------
# Wrapper
# ---------------------------
class RunMLModels(luigi.WrapperTask):
    def requires(self):
        return [TrainRecommendations(), ForecastSales()]