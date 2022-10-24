from logging import exception
from util.db_connection import Db_Connection
from util.configurationReader import readDataBaseConfigurations
from util.configurationReader import readRouteConfigurations
from datetime import datetime
import pandas as pd
import traceback


def load_channels(etl_sync_id):
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
        if ses_db_sor == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_sor == -3:
            raise Exception("Error trying connect to the database")
        
        #Dictionary for values of channels_tra
        channel_dict_tra = {
            "channel_id":[],
            "channel_desc":[],
            "channel_class":[],
            "channel_class_id":[]
        }
        #Dictionary for values of channels_dim
        channel_dict = {
            "channel_id":[],
            "channel_desc":[],
            "channel_class":[],
            "channel_class_id":[]
        }
        #Read tra table
        channel_tra = pd.read_sql(f"SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels_tra WHERE ETL_SYNC_ID = {etl_sync_id}", ses_db_stg)
        #Process the transform table
        if not channel_tra.empty:
            for id,des,cla,cla_id \
                in zip(channel_tra['CHANNEL_ID'],channel_tra['CHANNEL_DESC'],
                channel_tra['CHANNEL_CLASS'], channel_tra['CHANNEL_CLASS_ID']):
                channel_dict_tra["channel_id"].append(id)
                channel_dict_tra["channel_desc"].append(des)
                channel_dict_tra["channel_class"].append(cla)
                channel_dict_tra["channel_class_id"].append(cla_id)
        
        #Read sor table
        channel_sor = pd.read_sql(f"SELECT CHANNEL_ID, CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels_dim", ses_db_sor)
        #Process the transform table
        if not channel_sor.empty:
            for id,des,cla,cla_id \
                in zip(channel_sor['CHANNEL_ID'],channel_sor['CHANNEL_DESC'],
                channel_sor['CHANNEL_CLASS'], channel_sor['CHANNEL_CLASS_ID']):
                channel_dict["channel_id"].append(id)
                channel_dict["channel_desc"].append(des)
                channel_dict["channel_class"].append(cla)
                channel_dict["channel_class_id"].append(cla_id)
        
        if channel_dict["channel_id"]:
            df_channels_tra = pd.DataFrame(channel_dict_tra)
            df_channels_sor = pd.DataFrame(channel_dict)
            df_channels_dim = df_channels_tra.merge(df_channels_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            df_channels_dim.to_sql('channels_dim', ses_db_sor, if_exists="append",index=False)
        else:
            df_channels_dim = pd.DataFrame(channel_dict_tra)
            df_channels_dim.to_sql('channels_dim', ses_db_sor, if_exists="append",index=False)
        
        ses_db_stg.dispose()
        ses_db_sor.dispose()
    except:
        traceback.print_exc()
    finally:
        pass