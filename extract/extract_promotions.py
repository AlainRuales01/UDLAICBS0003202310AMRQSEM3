from logging import exception
from util.db_connection import Db_Connection
from datetime import datetime
import pandas as pd
import traceback

def ext_promotions():
    try:
        #Variables
        type = 'mysql'
        host = 'localhost'
        port = '3306'
        user = 'AdminUser'
        pwd = 'Admin123'
        db = 'amrqdbstg'

        con_db_stg = Db_Connection(type,host,port,user,pwd,db)
        ses_db_stg = con_db_stg.start()
        if ses_db_stg == -1:
            raise Exception(f"The give database type {type} is not valid")
        elif ses_db_stg == -3:
            raise Exception("Error trying connect to the database")

        #Dictionary for values of channels_ext
        promotions_dict = {
            "promo_id":[],
            "promo_name":[],
            "promo_cost":[],
            "promo_begin_date":[],
            "promo_end_date":[]
        }
        promotion_csv = pd.read_csv("CSV/promotions.csv")
        #Process CSV Content
        if not promotion_csv.empty:
            for id,nam,cos,beg_dat,end_dat \
                in zip(promotion_csv['PROMO_ID'],promotion_csv['PROMO_NAME'],
                promotion_csv['PROMO_COST'], promotion_csv['PROMO_BEGIN_DATE'],
                promotion_csv['PROMO_END_DATE']):

                promotions_dict["promo_id"].append(id)
                promotions_dict["promo_name"].append(nam)
                promotions_dict["promo_cost"].append(cos)
                promotions_dict["promo_begin_date"].append(beg_dat)
                promotions_dict["promo_end_date"].append(end_dat)

        if promotions_dict["promo_id"]:
            # ses_db_stg.connect().execute("TRUNCATE TABLE channels_ext")
            df_countries_ext = pd.DataFrame(promotions_dict)
            df_countries_ext.to_sql('promotions_ext', ses_db_stg, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass