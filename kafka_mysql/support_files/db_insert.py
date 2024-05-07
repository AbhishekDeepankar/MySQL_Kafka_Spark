import pandas as pd
import datetime
import kafka_mysql.Config.credentials as c
import kafka_mysql.Config.constants as cn
from kafka_mysql.helper_functions.generate_mysql_engine import get_mysql_engine

if __name__ == "__main__":
    current_timestamp = datetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    engine = get_mysql_engine(c.mysql_user1, c.mysql_user_password1,
                              host=cn.pipeln_db_host, database=cn.pipeln_db)

    df = pd.read_csv("file_path")
    df['created_on'] = current_timestamp
    df['updated_on'] = current_timestamp
    df.to_sql('table_name', con=engine, if_exists='append', index=False)
    # df = df.head(1)
    print(df)
    engine.dispose()
    
