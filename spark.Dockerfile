FROM apache/spark:4.1.1

USER root

# Install Python dependencies
RUN pip install --no-cache-dir \
    python-dotenv \
    psycopg2-binary \
    pandas \
    numpy

# Add PostgreSQL JDBC driver
ADD https://jdbc.postgresql.org/download/postgresql-42.7.4.jar /opt/spark/jars/
