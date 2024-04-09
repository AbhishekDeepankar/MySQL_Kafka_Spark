from sqlalchemy import create_engine
import sqlalchemy
import urllib.parse

def get_mysql_engine(
        user: str,
        passwd: str,
        host: str,
        database: str
) -> sqlalchemy.engine.base.Engine:

    passwd = urllib.parse.quote_plus(passwd)

    engine = create_engine("mysql+pymysql://" + user + ":" +
                           passwd + "@" + host + "/" + database)

    return engine
