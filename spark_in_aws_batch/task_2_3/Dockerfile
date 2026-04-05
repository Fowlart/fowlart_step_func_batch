# Use the official lightweight Python image
FROM python:3.11-slim

# Install Java and curl
RUN apt-get update && \
    apt-get install -y default-jre-headless procps curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy the built wheel package to /tmp/
COPY dist/mdm_spark-*.whl /tmp/

# Install the wheel package along with its dependencies
RUN pip install /tmp/mdm_spark-*.whl

# Configure PySpark JARs directory path
ENV SPARK_JARS_DIR=/usr/local/lib/python3.11/site-packages/pyspark/jars

# Download S3 connectors directly into PySpark's jars folder
WORKDIR ${SPARK_JARS_DIR}
RUN curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# Set working directory
WORKDIR /app

# Disable Python output buffering to ensure logs reach CloudWatch immediately
ENV PYTHONUNBUFFERED=1

# Define the core executable
ENTRYPOINT ["python"]