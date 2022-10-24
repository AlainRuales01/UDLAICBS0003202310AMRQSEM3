from logging import exception
from util.db_connection import Db_Connection
from util.configurationReader import readDataBaseConfigurations
from datetime import datetime
from transform.transformations import obt_date
import pandas as pd
import traceback

def load_promotions(etl_sync_id):
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
        #Dictionary for values of promotions_tra
        promotions_dict_tra = {
            "promo_id":[],
            "promo_name":[],
            "promo_cost":[],
            "promo_begin_date":[],
            "promo_end_date":[]
        }
        #Dictionary for values of promotions_dim
        promotions_dict = {
            "promo_id":[],
            "promo_name":[],
            "promo_cost":[],
            "promo_begin_date":[],
            "promo_end_date":[]
        }
        promotion_tra = pd.read_sql(f"SELECT PROMO_ID, PROMO_NAME, PROMO_COST, PROMO_BEGIN_DATE, PROMO_END_DATE FROM promotions_tra WHERE ETL_SYNC_ID = {etl_sync_id}", ses_db_stg)
        #Process Content
        if not promotion_tra.empty:
            for id,nam,cos,beg_dat,end_dat \
                in zip(promotion_tra['PROMO_ID'],promotion_tra['PROMO_NAME'],
                promotion_tra['PROMO_COST'], promotion_tra['PROMO_BEGIN_DATE'],
                promotion_tra['PROMO_END_DATE']):

                promotions_dict_tra["promo_id"].append(id)
                promotions_dict_tra["promo_name"].append(nam)
                promotions_dict_tra["promo_cost"].append(cos)
                promotions_dict_tra["promo_begin_date"].append(beg_dat)
                promotions_dict_tra["promo_end_date"].append(end_dat)
    
        promotion_sor = pd.read_sql(f"SELECT PROMO_ID, PROMO_NAME, PROMO_COST, PROMO_BEGIN_DATE, PROMO_END_DATE FROM promotions_dim", ses_db_sor)
        if not promotion_sor.empty:
            for id,nam,cos,beg_dat,end_dat \
                in zip(promotion_sor['PROMO_ID'],promotion_sor['PROMO_NAME'],
                promotion_sor['PROMO_COST'], promotion_sor['PROMO_BEGIN_DATE'],
                promotion_sor['PROMO_END_DATE']):

                promotions_dict["promo_id"].append(id)
                promotions_dict["promo_name"].append(nam)
                promotions_dict["promo_cost"].append(cos)
                promotions_dict["promo_begin_date"].append(beg_dat)
                promotions_dict["promo_end_date"].append(end_dat)
        
        if promotions_dict["promo_id"]:
            df_promotions_tra = pd.DataFrame(promotions_dict_tra)
            df_promotions_sor = pd.DataFrame(promotions_dict)
            df_promotions_dim = df_promotions_tra.merge(df_promotions_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            df_promotions_dim.to_sql('promotions_dim', ses_db_sor, if_exists="append",index=False)
        else:
            df_promotions_dim = pd.DataFrame(promotions_dict_tra)
            df_promotions_dim.to_sql('promotions_dim', ses_db_sor, if_exists="append",index=False)
        
        ses_db_stg.dispose()
        ses_db_sor.dispose()

    except:
        traceback.print_exc()
    finally:
        pass