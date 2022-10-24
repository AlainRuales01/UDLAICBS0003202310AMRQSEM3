from logging import exception
from util.db_connection import Db_Connection
from util.configurationReader import readDataBaseConfigurations
import traceback

def deleteData():
    dbConfiguration = readDataBaseConfigurations()
    try:
        #Variables
        type = dbConfiguration["DB_TYPE"]
        host = dbConfiguration["DB_HOST"]
        port = dbConfiguration["DB_PORT"]
        user = dbConfiguration["DB_USER"]
        pwd = dbConfiguration["DB_PWD"]
        db = dbConfiguration["DB_TARGET_STG"]

        con_db_stg = Db_Connection(type,host,port,user,pwd,db)
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -3:
            raise Exception("Error trying connect to the database")

        
        ses_db_stg.connect()
        ses_db_stg.execute("TRUNCATE TABLE sales_ext")
        ses_db_stg.execute("TRUNCATE TABLE customers_ext")
        ses_db_stg.execute("TRUNCATE TABLE channels_ext")
        ses_db_stg.execute("TRUNCATE TABLE countries_ext")
        ses_db_stg.execute("TRUNCATE TABLE products_ext")
        ses_db_stg.execute("TRUNCATE TABLE promotions_ext")
        ses_db_stg.execute("TRUNCATE TABLE times_ext")
        ses_db_stg.dispose()

    except:
        traceback.print_exc()
    finally:
        pass