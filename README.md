# Kafka_MySQL
This is a data streaming pipeline from mysql to kafka to spark to filesystem. The pipeline will capture any updates/inserts in the mysql database and publish to a Kafka topic, then the data will be consumed by spark, transfored if required. Spark will write the data to S3 or any other designated storage.

# Technologies
1. Mysql (8.3.0)
2. Kafka (7.4.4-ccs)
3. Pyspark (3.5.0)
4. Python (3.10)
5. Docker (4.28.0)
6. PyMySQL (1.1.0)
7. SQLAlchemy (2.0.29)

# setup

1. Use the docker compose files for setting up Mysql and Kafka

