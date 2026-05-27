# 1. Usamos la imagen base con Java 17
FROM eclipse-temurin:17-jdk-jammy

# 2. Variables de entorno de versiones
ENV SPARK_VERSION=4.1.1
ENV HADOOP_VERSION=3

# 3. Instalamos dependencias del sistema, Python 3 y pip
RUN apt-get update && apt-get install -y \
    curl \
    procps \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# 4. Descargamos e instalamos Apache Spark 4.1.1
RUN curl -O https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# 5. Configuramos las variables de entorno de Spark en el sistema
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# 6. Forzamos a Spark a utilizar explícitamente el ejecutable de Python 3
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# 7. Creamos el directorio de trabajo dentro del contenedor
WORKDIR /app

# 8. Copiamos e instalamos los requerimientos de Python
COPY spark-train-model/requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# 9. Copiamos el script de PySpark al contenedor
COPY spark-train-model/train_spark_mllib_model.py /app/train_spark_mllib_model.py