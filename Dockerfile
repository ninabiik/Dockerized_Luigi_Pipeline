# MySQL base image
FROM mysql:8.0

# --- System deps ---
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
      python3 python3-pip supervisor && \
    rm -rf /var/lib/apt/lists/*

# --- Python deps ---
RUN pip3 install --no-cache-dir \
    luigi==3.4.0 \
    pandas==2.2.2 \
    SQLAlchemy==2.0.31 \
    pymysql==1.1.0 \
    pyyaml==6.0.2

# --- App files ---
WORKDIR /app
# Copy your Luigi pipeline
COPY etl_luigi.py /app/etl_luigi.py
# Optional defaults (you can also pass params via CLI)
COPY luigi.cfg /app/luigi.cfg

# --- MySQL init: create DB on first startup ---
# Anything in this dir runs once when the MySQL datadir is empty
COPY init.sql /docker-entrypoint-initdb.d/init.sql

# --- Supervisor config to run mysqld + luigid together ---
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Expose MySQL and Luigi scheduler ports
EXPOSE 3306 8082

# Default env (override at runtime!)
ENV MYSQL_ROOT_PASSWORD=db_password

# Start both processes
CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
