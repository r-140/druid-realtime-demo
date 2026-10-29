FROM flink:1.20.1-scala_2.12-java17
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv python3-dev build-essential curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && python3 -m venv /opt/pyflink \
 && /opt/pyflink/bin/pip install --no-cache-dir apache-flink==1.20.1
RUN curl --fail --location --retry 3 \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar \
    -o /opt/flink/lib/flink-sql-connector-kafka-3.3.0-1.20.jar \
 && mkdir -p /opt/flink/state/checkpoints /opt/flink/state/savepoints /opt/flink/usrlib \
 && chown -R flink:flink /opt/flink/state /opt/flink/usrlib
ENV PATH="/opt/pyflink/bin:${PATH}" \
    PYFLINK_PYTHON=/opt/pyflink/bin/python \
    PYFLINK_CLIENT_EXECUTABLE=/opt/pyflink/bin/python
COPY --chown=flink:flink flink/stream_job.py flink/stream_contract.py /opt/flink/usrlib/
USER flink
