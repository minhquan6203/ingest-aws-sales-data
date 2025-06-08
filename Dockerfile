FROM bitnami/spark:3.3.2

USER root
WORKDIR /app

# Cài pip, Python packages và PostgreSQL JDBC driver
COPY requirements.txt .
RUN install_packages python3-pip wget && \
    pip install --no-cache-dir -r requirements.txt && \
    mkdir -p /opt/bitnami/spark/jars && \
    wget -O /opt/bitnami/spark/jars/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

COPY . .
RUN mkdir -p data logs

# Download data (có thể tách bước này ra để build nhẹ hơn)
RUN wget -O data/SalesData.csv https://raw.githubusercontent.com/aws-samples/data-engineering-on-aws/main/dataset/SalesData.csv

ENTRYPOINT ["python", "-m", "src.main"]
CMD ["--run-aws-sales", "--full-load"]
