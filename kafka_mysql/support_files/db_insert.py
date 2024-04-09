import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
import datetime


if __name__ == "__main__":
    current_timestamp = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    password = urllib.parse.quote_plus("God_Zilla1")  # '123%40456'

    engine = create_engine("mysql+pymysql://" + "hive" + ":" +
                           "God_Zilla1" + "@" + "192.168.20.10" + "/" + "pipeline_pool")

    # df = pd.read_sql('SELECT * FROM ELECTRICITY_PRODUCTION', con=engine)
    #
    pd.set_option('display.max_columns', None)
    df = pd.read_csv("D:\\Datasets\\global_electricity_production_data.csv")
    df['created_on'] = current_timestamp
    df['updated_on'] = current_timestamp
    df = df.head(2)
    df.to_sql('ELECTRICITY_PRODUCTION', con=engine, if_exists='append', index=False)
    print(df)
    engine.dispose()