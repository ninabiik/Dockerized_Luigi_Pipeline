import os
import re
import json
import luigi
import pandas as pd
from typing import Dict, List, Tuple
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# ---------- Helpers

def make_engine(host: str, port: int, user: str, password: str, db_name: str | None) -> Engine:
    # If db_name is None, connect to server-level (for CREATE DATABASE IF NOT EXISTS)
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/"
    if db_name:
        url += db_name
    return create_engine(url, future=True)

def normalize_columns(cols: List[str]) -> Tuple[List[str], Dict[str, str]]:
    """
    Normalize headers:
      - strip
      - lowercase
      - replace spaces & hyphens with underscores
      - remove non [a-z0-9_]
      - collapse repeated underscores
      - rstrip underscores
    Returns (normalized_cols, mapping original->normalized)
    Raises if duplicates appear after normalization.
    """
    mapping = {}
    normed = []
    for c in cols:
        orig = c
        x = c.strip().lower()
        x = x.replace("-", "_").replace(" ", "_")
        x = re.sub(r"[^a-z0-9_]", "", x)
        x = re.sub(r"_+", "_", x).rstrip("_")
        if not x:
            x = "col"
        # prevent reserved leading digits
        if re.match(r"^\d", x):
            x = f"c_{x}"
        mapping[orig] = x
        normed.append(x)

    # Check duplicates post-normalization
    seen = {}
    dups = set()
    for i, c in enumerate(normed):
        if c in seen:
            dups.add(c)
        else:
            seen[c] = i
    if dups:
        raise ValueError(
            f"Duplicate column names detected after normalization: {sorted(list(dups))}. "
            f"Original->normalized map: {json.dumps(mapping)}"
        )

    return normed, mapping

def load_dataframe_to_mysql(df: pd.DataFrame, engine: Engine, table_name: str, if_exists: str = "replace"):
    # Pandas will infer dtypes; you can extend this by passing a dtype dict if needed.
    df.to_sql(table_name, con=engine, if_exists=if_exists, index=False, method="multi", chunksize=10_000)


# ---------- Luigi Config

class ETLConfig(luigi.Config):
    data_dir = luigi.Parameter(
        default="/Users/nina/Downloads/datasets", #You can change this into your local directory
        description="Folder with CSVs"
    )
    db_host = luigi.Parameter(default="localhost")
    db_port = luigi.IntParameter(default=3306)
    db_user = luigi.Parameter(default="root")
    db_password = luigi.Parameter(default="changeme") #dont forget to change this
    db_name = luigi.Parameter(default="ecommerce")
    load_mode = luigi.Parameter(default="replace")  # 'replace' or 'append'


# ---------- Tasks

class EnsureDatabase(luigi.Task):
    """Creates the MySQL database if it doesn't exist."""
    def output(self):
        cfg = ETLConfig()
        marker = f".luigi_db_created_{cfg.db_name}.marker"
        return luigi.LocalTarget(marker)

    def run(self):
        cfg = ETLConfig()
        srv_engine = make_engine(cfg.db_host, cfg.db_port, cfg.db_user, cfg.db_password, db_name=None)
        with srv_engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{cfg.db_name}`"))
            conn.commit()
        with self.output().open("w") as f:
            f.write("created")


class ValidateAndTransformCSV(luigi.Task):
    """
    Reads a CSV, validates & normalizes column names, writes a normalized temporary parquet as output.
    """
    filename = luigi.Parameter()

    def requires(self):
        return EnsureDatabase()

    def output(self):
        base = os.path.splitext(os.path.basename(self.filename))[0]
        out_path = os.path.join(".luigi_work", f"{base}.parquet")
        os.makedirs(os.path.dirname(out_path), exist_ok=True)
        return luigi.LocalTarget(out_path)

    def run(self):
        cfg = ETLConfig()
        csv_path = os.path.join(cfg.data_dir, self.filename)
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV not found: {csv_path}")

        df = pd.read_csv(csv_path)

        # validate & normalize columns
        normed_cols, mapping = normalize_columns(list(df.columns))
        df.columns = normed_cols

        # (Optional) Basic sanity checks; add more rules here if you want strict schemas per file
        # Example: ensure an 'id' or '..._id' exists; comment out if not needed
        # if not any(col == "id" or col.endswith("_id") for col in df.columns):
        #     raise ValueError(f"{self.filename}: missing an id-like column")

        # Save normalized dataset to an intermediate parquet file
        df.to_parquet(self.output().path, index=False)

        # Log what changed for traceability
        log_path = self.output().path + ".log"
        with open(log_path, "w") as f:
            f.write(json.dumps({"file": self.filename, "column_mapping": mapping}, indent=2))


class LoadToMySQL(luigi.Task):
    """Loads a normalized parquet into MySQL under a table named after the file (without extension)."""
    filename = luigi.Parameter()

    def requires(self):
        return ValidateAndTransformCSV(filename=self.filename)

    def output(self):
        base = os.path.splitext(os.path.basename(self.filename))[0]
        # Use a simple local marker to mark completion (idempotency strategy is replace/append at DB)
        marker = os.path.join(".luigi_work", f"{base}.loaded")
        return luigi.LocalTarget(marker)

    def run(self):
        cfg = ETLConfig()
        base = os.path.splitext(os.path.basename(self.filename))[0]
        table_name = base  # e.g., 'customers', 'orders', etc.

        df = pd.read_parquet(self.input().path)
        engine = make_engine(cfg.db_host, cfg.db_port, cfg.db_user, cfg.db_password, cfg.db_name)

        # Create schema/table if not exists; handled implicitly by pandas to_sql when if_exists='append/replace'
        if cfg.load_mode not in ("replace", "append"):
            raise ValueError("load_mode must be 'replace' or 'append'")

        load_dataframe_to_mysql(df, engine, table_name, if_exists=cfg.load_mode)

        with self.output().open("w") as f:
            f.write("loaded")


class LoadAll(luigi.WrapperTask):
    """
    Convenience task: run ETL for the fixed ecommerce file set.
    """
    def requires(self):
        files = [
            "customers.csv",
            "order_items.csv",
            "orders.csv",
            "payments.csv",
            "products.csv",
        ]
        return [LoadToMySQL(filename=f) for f in files]
