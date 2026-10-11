FROM apache/airflow:2.10.5
USER root
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java
ENV JAVA_HOME=/opt/java
USER airflow
RUN pip install --no-cache-dir pyspark==3.5.6 cassandra-driver==3.29.2
