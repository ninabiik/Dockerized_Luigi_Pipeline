# MySQL (Oracle Linux base)
FROM mysql:8.0

# ---- System deps (use microdnf on Oracle Linux) ----
RUN microdnf install -y python3 python3-pip && microdnf clean all

# ---- Python deps (pip) ----
# Install supervisor from pip to avoid OS package managers
# ---- Python deps (pip) ----
RUN pip3 install --no-cache-dir \
    luigi==3.4.0 \
    pandas==2.2.2 \
    SQLAlchemy==2.0.31 \
    pymysql==1.1.0 \
    pyyaml==6.0.2 \
    supervisor==4.2.5 \
    pyarrow==16.1.0 \
    cryptography==42.0.5 #added to fix error

# ---- App files ----
WORKDIR /app
COPY etl_luigi.py /app/etl_luigi.py
COPY luigi.cfg /app/luigi.cfg

# MySQL init: create DB at first startup
COPY init.sql /docker-entrypoint-initdb.d/init.sql

# Supervisor config to run mysqld + luigid
COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

# Ensure log dirs exist for supervisor (optional but nice)
RUN mkdir -p /var/log && touch /var/log/luigid_stdout.log /var/log/luigid_stderr.log \
    /var/log/mysql_supervisor_stdout.log /var/log/mysql_supervisor_stderr.log \
    /var/log/supervisord.log

EXPOSE 3306 8082

# Use an obviously-not-real default; override at runtime with -e
ENV MYSQL_ROOT_PASSWORD=changeme

CMD ["supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]
