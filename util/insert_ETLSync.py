from logging import exception
from xmlrpc.client import DateTime
from pymysql import Date
from util.db_connection import Db_Connection
from util.configurationReader import readDataBaseConfigurations
from util.configurationReader import readRouteConfigurations
from datetime import datetime
import pandas as pd
import traceback

def insert_ETLSync():
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

        #Dictionary for values of channels_tra
        etlSync_dict = {
            "synchronization_date":[]
        }
        #Read ext table
        #Process the ext table
        fechaActual = datetime.now()
        etlSync_dict["synchronization_date"].append(fechaActual)
        df_etlSync = pd.DataFrame(etlSync_dict)
        df_etlSync.to_sql('etlsync', ses_db_stg, if_exists="append",index=False)
        valuesId=ses_db_stg.execute('SELECT ID FROM ETLSync ORDER BY ID DESC LIMIT 1').scalar()
        ses_db_stg.dispose()
        return valuesId
    except:
        traceback.print_exc()
    finally:
        pass