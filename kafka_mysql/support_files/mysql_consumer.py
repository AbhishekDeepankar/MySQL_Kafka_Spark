import kafka



if __name__ == "__main__":
    # import logging
    #
    # # logging.basicConfig(level=logging.INFO)
    # # log_level = logging.DEBUG
    # # logging.basicConfig(level=log_level)
    # # log = logging.getLogger('kafka')
    # # log.setLevel(log_level)

    consumer = kafka.KafkaConsumer('pipeline_mysql', bootstrap_servers=['192.168.20.10:9092'],
                                   consumer_timeout_ms=1000,
                                   auto_offset_reset='earliest',
                                   enable_auto_commit=False
                                   )
    for message in consumer:
        print("Message", message)
        if message is not None:
            print(message.offset, message.value)
    print(consumer.topics())
