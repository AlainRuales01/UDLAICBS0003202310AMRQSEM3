from logging import exception
from util.db_connection import Db_Connection
from util.configurationReader import readDataBaseConfigurations
from util.configurationReader import readRouteConfigurations
from datetime import datetime
import pandas as pd
import traceback


def load_channels():
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

        type = dbConfiguration["DB_TYPE"]
        host = dbConfiguration["DB_HOST"]
        port = dbConfiguration["DB_PORT"]
        user = dbConfiguration["DB_USER"]
        pwd = dbConfiguration["DB_PWD"]
        db = dbConfiguration["DB_TARGET_SOR"]

        con_db_sor = Db_Connection(type,host,port,user,pwd,db)
        ses_db_sor = con_db_sor.start()
        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -3:
            raise Exception("Error trying connect to the database")

        #Dictionary for values of dim_channels
        channel_dict = {
            "channel_id":[],
            "channel_desc":[],
            "channel_class":[],
            "channel_class_id":[]
        }
        #Read ext table
        channel_tra = pd.read_sql("SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels_tra", ses_db_stg)
        #Process the transform table
        if not channel_tra.empty:
            for id,des,cla,cla_id \
                in zip(channel_tra['CHANNEL_ID'],channel_tra['CHANNEL_DESC'],
                channel_tra['CHANNEL_CLASS'], channel_tra['CHANNEL_CLASS_ID']):
                channel_dict["channel_id"].append(id)
                channel_dict["channel_desc"].append(des)
                channel_dict["channel_class"].append(cla)
                channel_dict["channel_class_id"].append(cla_id)
        if channel_dict["channel_id"]:
            df_channels_tra = pd.DataFrame(channel_dict)
            df_channels_tra.to_sql('dim_channels', ses_db_sor, if_exists="append",index=False)
        ses_db_stg.dispose()
        ses_db_sor.dispose()
    except:
        traceback.print_exc()
    finally:
        pass