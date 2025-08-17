# Base: MySQL 8 (Oracle Linux base)
FROM mysql:8.0

# ---- System deps (use microdnf on Oracle Linux) ----
RUN microdnf install -y python3 python3-pip && microdnf clean all

# ---- Python deps ----
RUN pip3 install --no-cache-dir \
    luigi==3.4.0 \
    pandas==2.2.2 \
    SQLAlchemy==2.0.31 \
    pymysql==1.1.0 \
    pyyaml==6.0.2 \
    supervisor==4.2.5 \
    pyarrow==16.1.0 \
    cryptography==42.0.5

# ---- App files ----
WORKDIR /app
COPY etl_luigi.py /app/etl_luigi.py
COPY luigi.cfg /app/luigi.cfg

# ---- MySQL init script ----
COPY init.sql /docker-entrypoint-initdb.d/init.sql

# ---- Supervisor config ----
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Ensure log dirs exist for supervisor (optional but nice)
RUN mkdir -p /var/log && touch \
    /var/log/luigid_stdout.log /var/log/luigid_stderr.log \
    /var/log/mysql_supervisor_stdout.log /var/log/mysql_supervisor_stderr.log \
    /var/log/supervisord.log

# Ports: MySQL + Luigi scheduler
EXPOSE 3306 8082

# Default env (override at docker run)
ENV MYSQL_ROOT_PASSWORD=changeme

# Launch MySQL + luigid under supervisor
CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
