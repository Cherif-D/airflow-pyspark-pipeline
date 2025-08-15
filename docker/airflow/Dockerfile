FROM apache/airflow:2.9.1-python3.11

# Java 17 pour PySpark
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:${PATH}"
USER airflow

# ---- Installation OFFLINE (aucun accès internet) ----
# On copie les wheels construits/téléchargés sur l'hôte
COPY wheels/ /wheels/

# Installe d'abord py4j, puis pyspark, depuis les wheels locaux
RUN pip install --no-cache-dir /wheels/py4j-*.whl && \
    pip install --no-cache-dir /wheels/pyspark-*-py2.py3-none-any.whl
