import pandas as pd
from kafka_mysql.helper_functions.generate_mysql_engine import get_mysql_engine
import datetime
import kafka
import json
import kafka_mysql.Config.credentials as c
import kafka_mysql.Config.constants as cn


def kafka_producer(
        kafka_topic: str,
        database: str,
        broker: list[str],
        mysql_host: str,
        table: str,
        bookmark_tbl: str
) -> str:

    # creating a timestamp for the bookmark
    current_timestamp = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")

    # creating kafka producer with json serialization
    producer = kafka.KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # mysql engine for database I/O ops
    engine = get_mysql_engine(c.mysql_user1, c.mysql_user_password1, host=mysql_host, database=database)

    # Checking if there are any new records to be published to the topic
    max_date_df = pd.read_sql('select max(updated_on) as max_date FROM ' + table, con=engine)
    bookmark_data_df = pd.read_sql('select ID from ' + bookmark_tbl, con=engine)
    bookmark_df = pd.read_sql('select max(updated_on) as max_date from ' + bookmark_tbl, con=engine)

    if bookmark_data_df.empty:
        # reading records from the mysql database
        df = pd.read_sql('SELECT country_name, date, parameter, product, value,'
                         'unit, created_on, updated_on, ID FROM ' + table, con=engine)
        if not df.empty:
            for row in df.itertuples():
                j_object = {
                    'country_name': row[1],
                    'date': row[2],
                    'parameter': row[3],
                    'product': row[4],
                    'value': row[5],
                    'unit': row[6],
                    'created_on': row[7],
                    'updated_on': row[8],
                    'ID': row[9]
                }

                # casting timestamp to string for JSON serialization
                j_object['created_on'] = j_object['created_on'].strftime('%Y-%m-%d %H:%M:%S')
                j_object['updated_on'] = j_object['updated_on'].strftime('%Y-%m-%d %H:%M:%S')

                producer.send(topic=kafka_topic, value=j_object)

            # updating bookmark table with the latest timestamp
            max_date = max_date_df['max_date'].dt.strftime('%Y-%m-%d %H:%M:%S').values[0]
            data = [[current_timestamp, max_date, table]]
            bookmark_df = pd.DataFrame(data, columns=['RUN_TIME', 'UPDATED_ON', 'TABLE_NAME'])
            bookmark_df.to_sql('BOOKMARK', con=engine, if_exists='append', index=False)
            producer.flush()

    else:
        max_date = bookmark_df['max_date'].dt.strftime('%Y-%m-%d %H:%M:%S').values[0]
        df = pd.read_sql("SELECT country_name, date, parameter, product, value,"
                         "unit, created_on, updated_on, ID FROM " + table +
                         " where updated_on > %s", con=engine, params=(max_date,))

        max_date_df = pd.read_sql("SELECT max(updated_on) as max_df_date FROM " + table +
                                  " where updated_on > %s", con=engine, params=(max_date,))

        if not df.empty:
            for row in df.itertuples():
                j_object = {
                    'country_name': row[1],
                    'date': row[2],
                    'parameter': row[3],
                    'product': row[4],
                    'value': row[5],
                    'unit': row[6],
                    'created_on': row[7],
                    'updated_on': row[8],
                    'ID': row[9]
                }

                j_object['created_on'] = j_object['created_on'].strftime('%Y-%m-%d %H:%M:%S')
                j_object['updated_on'] = j_object['updated_on'].strftime('%Y-%m-%d %H:%M:%S')

                producer.send(topic=kafka_topic, value=j_object)
            max_df_date = max_date_df['max_df_date'].dt.strftime('%Y-%m-%d %H:%M:%S').values[0]
            data = [[current_timestamp, max_df_date, table]]
            bookmark_df = pd.DataFrame(data, columns=['RUN_TIME', 'UPDATED_ON', 'TABLE_NAME'])
            bookmark_df.to_sql(bookmark_tbl, con=engine, if_exists='append', index=False)
            producer.flush()

    engine.dispose()
    return '0'


if __name__ == "__main__":

    while True:
        try:
            kafka_producer(
                kafka_topic=cn.pipeln_topic,
                database=cn.pipeln_db,
                broker=cn.pipeln_broker,
                mysql_host=cn.pipeln_db_host,
                table=cn.pipeln_table1,
                bookmark_tbl=cn.pipeln_bookmark_tbl
            )
        except Exception as err:
            print('Something unexpected -> ', err)
