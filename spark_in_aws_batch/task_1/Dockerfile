# 1. Використовуємо офіційний легкий образ Python
FROM python:3.11-slim

# 2. Встановлюємо Java, системні утиліти та curl (який потрібен для завантаження JAR)
RUN apt-get update && \
    apt-get install -y default-jre-headless procps curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*


RUN pip install --no-cache-dir pyspark

RUN pip install delta-spark==3.1.0

# 4. Налаштовуємо шлях до JAR-файлів PySpark
# У образі python:3.11-slim пакети встановлюються в /usr/local/lib/python3.11/site-packages/
ENV SPARK_JARS_DIR=/usr/local/lib/python3.11/site-packages/pyspark/jars

# 5. Завантажуємо конектори для S3 прямо в папку jars самого PySpark
# Це гарантує, що Spark автоматично знайде потрібні класи при старті
WORKDIR ${SPARK_JARS_DIR}
RUN curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# 6. Встановлюємо робочу директорію для твого коду
WORKDIR /app

# 7. Копіюємо всі Python скрипти у контейнер
COPY *.py .

ENTRYPOINT ["python"]

# Define the default arguments (fallback if ContainerOverrides is absent)
CMD ["task_1.py"]