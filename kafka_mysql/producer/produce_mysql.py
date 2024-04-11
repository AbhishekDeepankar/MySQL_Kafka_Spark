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

    current_timestamp = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    producer = kafka.KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    engine = get_mysql_engine(c.mysql_user1, c.mysql_user_password1, host=mysql_host, database=database)

    max_date_df = pd.read_sql('select max(updated_on) as max_date FROM ' + table, con=engine)

    bookmark_df = pd.read_sql('select max(updated_on) as max_date from ' + bookmark_tbl, con=engine)

    if bookmark_df.empty:
        df = pd.read_sql('SELECT country_name, date, parameter, product, value,'
                         'unit, created_on, updated_on, ID FROM ' + table, con=engine)
        if not df.empty:
            for row in df.itertuples():
                producer.send(topic=kafka_topic, value={
                    'country_name': row[1],
                    'date': row[2],
                    'parameter': row[0],
                    'product': row[0],
                    'value': row[0],
                    'unit': row[0],
                    'created_on': row[0],
                    'updated_on': row[0],
                    'ID': row[0]
                })
            max_date = max_date_df['max_date'].dt.strftime('%Y-%m-%d %H:%M:%S').values[0]
            data = [[current_timestamp, max_date, table]]
            bookmark_df = pd.DataFrame(data, columns=['RUN_TIME', 'UPDATED_ON', 'TABLE_NAME'])
            bookmark_df.to_sql('BOOKMARK', con=engine, if_exists='append', index=False)
            producer.flush()

    else:
        print(1)
        max_date = bookmark_df['max_date'].dt.strftime('%Y-%m-%d %H:%M:%S').values[0]
        df = pd.read_sql("SELECT country_name, date, parameter, product, value,"
                         "unit, created_on, updated_on, ID FROM " + table +
                         " where updated_on > %s", con=engine, params=(max_date,))

        max_date_df = pd.read_sql("SELECT max(updated_on) as max_df_date FROM " + table +
                                  " where updated_on > %s", con=engine, params=(max_date,))
        max_df_date = max_date_df['max_df_date'].dt.strftime('%Y-%m-%d %H:%M:%S').values[0]

        if not df.empty:
            for row in df.itertuples():
                producer.send(topic=kafka_topic, value={
                    'country_name': row[1],
                    'date': row[2],
                    'parameter': row[0],
                    'product': row[0],
                    'value': row[0],
                    'unit': row[0],
                    'created_on': row[0],
                    'updated_on': row[0],
                    'ID': row[0]
                })

            data = [[current_timestamp, max_df_date, table]]
            bookmark_df = pd.DataFrame(data, columns=['RUN_TIME', 'UPDATED_ON', 'TABLE_NAME'])
            bookmark_df.to_sql(bookmark_tbl, con=engine, if_exists='append', index=False)
            producer.flush()

    engine.dispose()
    return '0'


if __name__ == "__main__":
    kafka_producer(cn.pipeln_topic,
                   cn.pipeln_db,
                   cn.pipeln_broker,
                   cn.pipeln_db_host,
                   cn.pipeln_table1,
                   cn.pipeln_broker)
