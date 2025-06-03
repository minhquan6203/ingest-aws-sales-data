FROM python:3.9-slim

WORKDIR /app

# Install Java for PySpark
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download AWS SDK dependencies
RUN mkdir -p /usr/share/java && \
    cd /usr/share/java && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.261/aws-java-sdk-bundle-1.12.261.jar && \
    wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Create data directory for CSV files
RUN mkdir -p data

# Copy application code
COPY . .

# Create logs directory
RUN mkdir -p logs

# Download the SalesData.csv file
RUN wget -O data/SalesData.csv https://raw.githubusercontent.com/aws-samples/data-engineering-on-aws/main/dataset/SalesData.csv

# Set entrypoint to use our main script
ENTRYPOINT ["python", "-m", "src.main"]

# Default command - run AWS sales pipeline with full load
CMD ["--run-aws-sales", "--full-load"] 