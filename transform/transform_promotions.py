from logging import exception
from util.db_connection import Db_Connection
from util.configurationReader import readDataBaseConfigurations
from datetime import datetime
from transform.transformations import obt_date
import pandas as pd
import traceback

def tra_promotions(etl_sync_id):
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

        #Dictionary for values of promotions_tra
        promotions_dict = {
            "promo_id":[],
            "promo_name":[],
            "promo_cost":[],
            "promo_begin_date":[],
            "promo_end_date":[],
            "etl_sync_id":[]
        }
        promotion_tra = pd.read_sql("SELECT PROMO_ID, PROMO_NAME, PROMO_COST, PROMO_BEGIN_DATE, PROMO_END_DATE FROM promotions_ext", ses_db_stg)
        #Process CSV Content
        if not promotion_tra.empty:
            for id,nam,cos,beg_dat,end_dat \
                in zip(promotion_tra['PROMO_ID'],promotion_tra['PROMO_NAME'],
                promotion_tra['PROMO_COST'], promotion_tra['PROMO_BEGIN_DATE'],
                promotion_tra['PROMO_END_DATE']):

                promotions_dict["promo_id"].append(id)
                promotions_dict["promo_name"].append(nam)
                promotions_dict["promo_cost"].append(cos)
                promotions_dict["promo_begin_date"].append(obt_date(beg_dat))
                promotions_dict["promo_end_date"].append(obt_date((end_dat)))
                promotions_dict["etl_sync_id"].append(etl_sync_id)

        if promotions_dict["promo_id"]:
            df_promotions_tra = pd.DataFrame(promotions_dict)
            df_promotions_tra.to_sql('promotions_tra', ses_db_stg, if_exists="append",index=False)
        ses_db_stg.dispose()
    except:
        traceback.print_exc()
    finally:
        pass