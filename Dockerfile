FROM flink:2.2.0-java17
USER root

RUN apt-get update -y && \
    apt-get install -y \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    openjdk-17-jdk && \
    rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

# Set JAVA_HOME to the installed JDK
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# Install the Python libraries
RUN pip3 install --no-cache-dir --break-system-packages \
    apache-flink==2.2.0 \
    h3==4.1.0

# Copy Kafka connector JAR into Flink's lib directory
COPY jars/*.jar /opt/flink/lib/
