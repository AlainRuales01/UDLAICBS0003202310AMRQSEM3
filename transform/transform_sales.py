from logging import exception
from util.db_connection import Db_Connection
from util.configurationReader import readDataBaseConfigurations
from transform.transformations import obt_date
from datetime import datetime
import pandas as pd
import traceback

def tra_sales(etl_sync_id):
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

        #Dictionary for values of sales_tra
        sales_dict = {
            "prod_id":[],
            "cust_id":[],
            "time_id":[],
            "channel_id":[],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[],
            "etl_sync_id":[]
        }

        sale_tra = pd.read_sql("SELECT PROD_ID, CUST_ID, TIME_ID, CHANNEL_ID, PROMO_ID, QUANTITY_SOLD, AMOUNT_SOLD FROM sales_ext", ses_db_stg)
        #Process CSV Content
        if not sale_tra.empty:
            for pro_id,cus_id,tim_id,cha_id,prom_id,qua_sol,amo_sol \
                in zip(sale_tra['PROD_ID'],sale_tra['CUST_ID'],
                sale_tra['TIME_ID'], sale_tra['CHANNEL_ID'],
                sale_tra['PROMO_ID'],sale_tra['QUANTITY_SOLD'],
                sale_tra['AMOUNT_SOLD']):

                sales_dict["prod_id"].append(pro_id)
                sales_dict["cust_id"].append(cus_id)
                sales_dict["time_id"].append(obt_date(tim_id))
                sales_dict["channel_id"].append(cha_id)
                sales_dict["promo_id"].append(prom_id)
                sales_dict["quantity_sold"].append(qua_sol)
                sales_dict["amount_sold"].append(amo_sol)
                sales_dict["etl_sync_id"].append(etl_sync_id)

        if sales_dict["prod_id"]:
            df_sales_tra = pd.DataFrame(sales_dict)
            df_sales_tra.to_sql('sales_tra', ses_db_stg, if_exists="append",index=False)
        ses_db_stg.dispose()
    except:
        traceback.print_exc()
    finally:
        pass