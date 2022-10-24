from logging import exception
from os import times_result
from util.db_connection import Db_Connection
from util.configurationReader import readDataBaseConfigurations
from transform.transformations import obt_date
from datetime import datetime
import pandas as pd
import traceback

def load_sales(etl_sync_id):
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
        
        #Dictionary for product key
        prod_sor = pd.read_sql(f"SELECT ID, PROD_ID FROM products_dim", ses_db_sor)
        product_dict = dict()
        if not prod_sor.empty:
            for id,pro_id \
                in zip(prod_sor['ID'],prod_sor['PROD_ID']):
                product_dict[pro_id] = id
        
        cust_sor = pd.read_sql(f"SELECT ID, CUST_ID FROM customers_dim", ses_db_sor)
        #Dictionary for customer key
        customer_dict = dict()
        if not cust_sor.empty:
            for id,cus_id \
                in zip(cust_sor['ID'],cust_sor['CUST_ID']):
                customer_dict[cus_id] = id
        
        time_sor = pd.read_sql(f"SELECT ID, TIME_ID FROM times_dim", ses_db_sor)
        #Dictionary for time key
        time_dict = dict()
        if not time_sor.empty:
            for id,tim_id \
                in zip(time_sor['ID'],time_sor['TIME_ID']):
                time_dict[tim_id] = id
        
        chan_sor = pd.read_sql(f"SELECT ID, CHANNEL_ID FROM channels_dim", ses_db_sor)
        #Dictionary for channel key
        channel_dict = dict()
        if not chan_sor.empty:
            for id,cha_id \
                in zip(chan_sor['ID'],chan_sor['CHANNEL_ID']):
                channel_dict[cha_id] = id

        promo_sor = pd.read_sql(f"SELECT ID, PROMO_ID FROM promotions_dim", ses_db_sor)
        #Dictionary for promo key
        promotion_dict = dict()
        if not promo_sor.empty:
            for id,pro_id \
                in zip(promo_sor['ID'],promo_sor['PROMO_ID']):
                promotion_dict[pro_id] = id

        #Dictionary for values of sales_tra
        sales_dict_tra = {
            "prod_id":[],
            "cust_id":[],
            "time_id":[],
            "channel_id":[],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[],
        }

        sales_dict = {
            "prod_id":[],
            "cust_id":[],
            "time_id":[],
            "channel_id":[],
            "promo_id":[],
            "quantity_sold":[],
            "amount_sold":[],
        }

        sale_tra = pd.read_sql(f"SELECT PROD_ID, CUST_ID, TIME_ID, CHANNEL_ID, PROMO_ID, QUANTITY_SOLD, AMOUNT_SOLD FROM sales_tra WHERE ETL_SYNC_ID = {etl_sync_id} ", ses_db_stg)
        #Process CSV Content
        if not sale_tra.empty:
            for pro_id,cus_id,tim_id,cha_id,prom_id,qua_sol,amo_sol \
                in zip(sale_tra['PROD_ID'],sale_tra['CUST_ID'],
                sale_tra['TIME_ID'], sale_tra['CHANNEL_ID'],
                sale_tra['PROMO_ID'],sale_tra['QUANTITY_SOLD'],
                sale_tra['AMOUNT_SOLD']):

                sales_dict_tra["prod_id"].append(product_dict[pro_id])
                sales_dict_tra["cust_id"].append(customer_dict[cus_id])
                sales_dict_tra["time_id"].append(time_dict[tim_id])
                sales_dict_tra["channel_id"].append(channel_dict[cha_id])
                sales_dict_tra["promo_id"].append(promotion_dict[prom_id])
                sales_dict_tra["quantity_sold"].append(qua_sol)
                sales_dict_tra["amount_sold"].append(amo_sol)

        sale_sor = pd.read_sql("SELECT PROD_ID, CUST_ID, TIME_ID, CHANNEL_ID, PROMO_ID, QUANTITY_SOLD, AMOUNT_SOLD FROM sales", ses_db_sor)
        if not sale_sor.empty:
            for pro_id,cus_id,tim_id,cha_id,prom_id,qua_sol,amo_sol \
                in zip(sale_sor['PROD_ID'],sale_sor['CUST_ID'],
                sale_sor['TIME_ID'], sale_sor['CHANNEL_ID'],
                sale_sor['PROMO_ID'],sale_sor['QUANTITY_SOLD'],
                sale_sor['AMOUNT_SOLD']):

                sales_dict["prod_id"].append(pro_id)
                sales_dict["cust_id"].append(cus_id)
                sales_dict["time_id"].append(tim_id)
                sales_dict["channel_id"].append(cha_id)
                sales_dict["promo_id"].append(prom_id)
                sales_dict["quantity_sold"].append(qua_sol)
                sales_dict["amount_sold"].append(amo_sol)
        if sales_dict["prod_id"]:
            df_sales_tra = pd.DataFrame(sales_dict_tra)
            df_sales_sor = pd.DataFrame(sales_dict)
            df_sales_dim = df_sales_tra.merge(df_sales_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            df_sales_dim.to_sql('customers_dim', ses_db_sor, if_exists="append",index=False)
        else:
            df_sales_dim = pd.DataFrame(sales_dict_tra)
            df_sales_dim.to_sql('sales', ses_db_sor, if_exists="append",index=False)
        
        ses_db_stg.dispose()
        ses_db_sor.dispose()
          
    except:
        traceback.print_exc()
    finally:
        pass