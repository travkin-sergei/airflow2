FROM apache/airflow:2.10.5

USER root

# Установка необходимых системных зависимостей
RUN apt-get update && apt-get install -y \
    build-essential \
    python3-dev \
    libpq-dev \  # Для psycopg2
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Копируем файл requirements.txt в образ
COPY requirements.txt /requirements.txt

# Установка pip и зависимостей из requirements.txt от имени пользователя airflow
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt