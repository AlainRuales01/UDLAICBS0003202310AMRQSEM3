from logging import exception
from util.db_connection import Db_Connection
from util.configurationReader import readDataBaseConfigurations
from datetime import datetime
import pandas as pd
import traceback

def load_products(etl_sync_id):
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
        
        #Dictionary for values of products_tra
        products_dict_tra = {
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
            "prod_min_price":[]
        }
        #Dictionary for values of products_dim
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
            "prod_min_price":[]
        }

        product_tra = pd.read_sql(f"SELECT PROD_ID, PROD_NAME, PROD_DESC, PROD_CATEGORY,PROD_CATEGORY_ID,PROD_CATEGORY_DESC,PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS,PROD_LIST_PRICE,PROD_MIN_PRICE FROM products_tra WHERE ETL_SYNC_ID = {etl_sync_id}", ses_db_stg)
        #Process Content
        if not product_tra.empty:
            for id,nam,des,cat,cat_id,cat_desc,wei_cla,sup_id,sta,lis_pri,min_pri \
                in zip(product_tra['PROD_ID'],product_tra['PROD_NAME'],
                product_tra['PROD_DESC'], product_tra['PROD_CATEGORY'],
                product_tra['PROD_CATEGORY_ID'],product_tra['PROD_CATEGORY_DESC'],
                product_tra['PROD_WEIGHT_CLASS'],product_tra['SUPPLIER_ID'],
                product_tra['PROD_STATUS'],product_tra['PROD_LIST_PRICE'],
                product_tra['PROD_MIN_PRICE']):

                products_dict_tra["prod_id"].append(id)
                products_dict_tra["prod_name"].append(nam)
                products_dict_tra["prod_desc"].append(des)
                products_dict_tra["prod_category"].append(cat)
                products_dict_tra["prod_category_id"].append(cat_id)
                products_dict_tra["prod_category_desc"].append(cat_desc)
                products_dict_tra["prod_weight_class"].append(wei_cla)
                products_dict_tra["supplier_id"].append(sup_id)
                products_dict_tra["prod_status"].append(sta)
                products_dict_tra["prod_list_price"].append(lis_pri)
                products_dict_tra["prod_min_price"].append(min_pri)
      
        product_sor = pd.read_sql(f"SELECT PROD_ID, PROD_NAME, PROD_DESC, PROD_CATEGORY,PROD_CATEGORY_ID,PROD_CATEGORY_DESC,PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS,PROD_LIST_PRICE,PROD_MIN_PRICE FROM products_dim", ses_db_sor)
        if not product_sor.empty:
            for id,nam,des,cat,cat_id,cat_desc,wei_cla,sup_id,sta,lis_pri,min_pri \
                in zip(product_sor['PROD_ID'],product_sor['PROD_NAME'],
                product_sor['PROD_DESC'], product_sor['PROD_CATEGORY'],
                product_sor['PROD_CATEGORY_ID'],product_sor['PROD_CATEGORY_DESC'],
                product_sor['PROD_WEIGHT_CLASS'],product_sor['SUPPLIER_ID'],
                product_sor['PROD_STATUS'],product_sor['PROD_LIST_PRICE'],
                product_sor['PROD_MIN_PRICE']):

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
        
        if products_dict["prod_id"]:
            df_products_tra = pd.DataFrame(products_dict_tra)
            df_products_sor = pd.DataFrame(products_dict)
            df_products_dim = df_products_tra.merge(df_products_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            df_products_dim.to_sql('products_dim', ses_db_sor, if_exists="append",index=False)
        else:
            df_products_dim = pd.DataFrame(products_dict_tra)
            df_products_dim.to_sql('products_dim', ses_db_sor, if_exists="append",index=False)
        
        ses_db_stg.dispose()
        ses_db_sor.dispose()
        
        
        ses_db_stg.dispose()
    except:
        traceback.print_exc()
    finally:
        pass