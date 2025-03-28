FROM openjdk:8-jdk-slim

# Install dependencies
RUN apt-get update && apt-get install -y \
    python3-pip \
    python3-dev \
    curl \
    gnupg \
    libgfortran5 \
    && rm -rf /var/lib/apt/lists/*

# Install Spark
RUN curl -sL https://archive.apache.org/dist/spark/spark-3.1.2/spark-3.1.2-bin-hadoop3.2.tgz | tar -xz -C /opt
ENV SPARK_HOME=/opt/spark-3.1.2-bin-hadoop3.2
ENV PATH=$SPARK_HOME/bin:$PATH

# Install pyspark and other dependencies
RUN pip3 install pyspark cassandra-driver kafka-python

# Copy the spark_stream.py script into the container
COPY spark_stream.py /app/spark_stream.py

# Set working directory
WORKDIR /app

# Run the script
CMD ["python3", "spark_stream.py"]
