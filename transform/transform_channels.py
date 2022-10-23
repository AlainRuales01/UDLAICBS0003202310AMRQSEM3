from logging import exception
from util.db_connection import Db_Connection
from util.configurationReader import readDataBaseConfigurations
from datetime import datetime
import pandas as pd
import traceback


def tra_channels(etl_sync_id):
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
        channel_dict = {
            "channel_id":[],
            "channel_desc":[],
            "channel_class":[],
            "channel_class_id":[],
            "etl_sync_id":[]
        }
        #Read ext table
        channel_ext = pd.read_sql("SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels_ext", ses_db_stg)
        #Process the ext table
        if not channel_ext.empty:
            for id,des,cla,cla_id \
                in zip(channel_ext['CHANNEL_ID'],channel_ext['CHANNEL_DESC'],
                channel_ext['CHANNEL_CLASS'], channel_ext['CHANNEL_CLASS_ID']):
                channel_dict["channel_id"].append(id)
                channel_dict["channel_desc"].append(des)
                channel_dict["channel_class"].append(cla)
                channel_dict["channel_class_id"].append(cla_id)
                channel_dict["etl_sync_id"].append(etl_sync_id)
        if channel_dict["channel_id"]:
            df_channels_tra = pd.DataFrame(channel_dict)

            df_channels_tra.to_sql('channels_tra', ses_db_stg, if_exists="append",index=False)
        ses_db_stg.dispose()
    except:
        traceback.print_exc()
    finally:
        pass