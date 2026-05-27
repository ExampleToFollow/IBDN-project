# 1. Usamos la imagen base con Java 17
FROM eclipse-temurin:17-jdk-jammy

# 2. Variables de entorno de versiones
ENV SPARK_VERSION=4.1.1
ENV HADOOP_VERSION=3

# 3. Instalamos dependencias básicas del sistema
RUN apt-get update && apt-get install -y curl procps && rm -rf /var/lib/apt/lists/*

# 4. Descargamos e instalamos Spark 4.1.1 para tener disponible el comando 'spark-submit'
RUN curl -O https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# 5. Configuramos las variables de entorno de Spark en el sistema
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# 6. Creamos el directorio de trabajo dentro del contenedor
WORKDIR /app

# 7. Copiamos tu Fat JAR directamente al contenedor
COPY spark-kafka-prediction/target/scala-2.13/spark-kafka-predictor-assembly.jar /app/spark-kafka-predictor.jar

# 8. Quitamos el tail -f para heredar el comportamiento natural de Spark