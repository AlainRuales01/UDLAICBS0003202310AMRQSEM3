from logging import exception
from util.db_connection import Db_Connection
from util.configurationReader import readDataBaseConfigurations
from datetime import datetime
import pandas as pd
import traceback

def tra_products(etl_sync_id):
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

        #Dictionary for values of products_tra
        products_dict = {
            "prod_id":[],
            "prod_name":[],
            "prod_desc":[],
            "prod_category":[],
            "prod_category_id":[],
            "prod_category_desc":[],
            "prod_weight_class":[],
            "supplier_id":[],
            "prod_status":[],
            "prod_list_price":[],
            "prod_min_price":[],
            "etl_sync_id":[],
            
        }

        product_tra = pd.read_sql("SELECT PROD_ID, PROD_NAME, PROD_DESC, PROD_CATEGORY,PROD_CATEGORY_ID,PROD_CATEGORY_DESC,PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS,PROD_LIST_PRICE,PROD_MIN_PRICE FROM products_ext", ses_db_stg)
        #Process CSV Content
        if not product_tra.empty:
            for id,nam,des,cat,cat_id,cat_desc,wei_cla,sup_id,sta,lis_pri,min_pri \
                in zip(product_tra['PROD_ID'],product_tra['PROD_NAME'],
                product_tra['PROD_DESC'], product_tra['PROD_CATEGORY'],
                product_tra['PROD_CATEGORY_ID'],product_tra['PROD_CATEGORY_DESC'],
                product_tra['PROD_WEIGHT_CLASS'],product_tra['SUPPLIER_ID'],
                product_tra['PROD_STATUS'],product_tra['PROD_LIST_PRICE'],
                product_tra['PROD_MIN_PRICE']):

                products_dict["prod_id"].append(id)
                products_dict["prod_name"].append(nam)
                products_dict["prod_desc"].append(des)
                products_dict["prod_category"].append(cat)
                products_dict["prod_category_id"].append(cat_id)
                products_dict["prod_category_desc"].append(cat_desc)
                products_dict["prod_weight_class"].append(wei_cla)
                products_dict["supplier_id"].append(sup_id)
                products_dict["prod_status"].append(sta)
                products_dict["prod_list_price"].append(lis_pri)
                products_dict["prod_min_price"].append(min_pri)
                products_dict["etl_sync_id"].append(etl_sync_id)
        if products_dict["prod_id"]:
            df_products_tra = pd.DataFrame(products_dict)
            df_products_tra.to_sql('products_tra', ses_db_stg, if_exists="append",index=False)
        ses_db_stg.dispose()
    except:
        traceback.print_exc()
    finally:
        pass