import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
from kafka import KafkaProducer
import json
import kafka
from json import dumps


if __name__ == "__main__":
    # import logging
    #
    # # logging.basicConfig(level=logging.INFO)
    # # log_level = logging.DEBUG
    # # logging.basicConfig(level=log_level)
    # # log = logging.getLogger('kafka')
    # # log.setLevel(log_level)

    producer = kafka.KafkaProducer(
        bootstrap_servers=['192.168.20.10:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    producer.send(
        'pipeline_mysql',
        value=
        {
            "name": "Giuseppe Rossi",
            "hotel": "Luxury Hotel",
            "dateFrom": "25-06-2021",
            "dateTo": "07-07-2021",
            "details": "I want the best room !!!!"
        }
    )
    # producer.send('pipeline_mysql', b"3455")
    producer.flush()
    exit(0)
