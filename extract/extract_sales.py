from logging import exception
from util.db_connection import Db_Connection
from datetime import datetime
import pandas as pd
import traceback

def ext_sales():
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
        sales_dict = {
            "prod_id":[],
            "cust_id":[],
            "time_id":[],
            "channel_id":[],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[]
        }

        sale_csv = pd.read_csv("CSV/sales.csv")
        #Process CSV Content
        if not sale_csv.empty:
            for pro_id,cus_id,tim_id,cha_id,prom_id,qua_sol,amo_sol \
                in zip(sale_csv['PROD_ID'],sale_csv['CUST_ID'],
                sale_csv['TIME_ID'], sale_csv['CHANNEL_ID'],
                sale_csv['PROMO_ID'],sale_csv['QUANTITY_SOLD'],
                sale_csv['AMOUNT_SOLD']):

                sales_dict["prod_id"].append(pro_id)
                sales_dict["cust_id"].append(cus_id)
                sales_dict["time_id"].append(tim_id)
                sales_dict["channel_id"].append(cha_id)
                sales_dict["promo_id"].append(prom_id)
                sales_dict["quantity_sold"].append(qua_sol)
                sales_dict["amount_sold"].append(amo_sol)


        if sales_dict["prod_id"]:
            # ses_db_stg.connect().execute("TRUNCATE TABLE channels_ext")
            df_countries_ext = pd.DataFrame(sales_dict)
            df_countries_ext.to_sql('sales_ext', ses_db_stg, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass