from logging import exception
from util.db_connection import Db_Connection
from util.configurationReader import readDataBaseConfigurations
from util.configurationReader import readRouteConfigurations
from datetime import datetime
import pandas as pd
import traceback

def ext_products():
    dbConfiguration = readDataBaseConfigurations()
    routeConfiguration = readRouteConfigurations()
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

        #Dictionary for values of channels_ext
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

        product_csv = pd.read_csv("{}products.csv".format(routeConfiguration["CSV_ROUTE"]))
        #Process CSV Content
        if not product_csv.empty:
            for id,nam,des,cat,cat_id,cat_desc,wei_cla,sup_id,sta,lis_pri,min_pri \
                in zip(product_csv['PROD_ID'],product_csv['PROD_NAME'],
                product_csv['PROD_DESC'], product_csv['PROD_CATEGORY'],
                product_csv['PROD_CATEGORY_ID'],product_csv['PROD_CATEGORY_DESC'],
                product_csv['PROD_WEIGHT_CLASS'],product_csv['SUPPLIER_ID'],
                product_csv['PROD_STATUS'],product_csv['PROD_LIST_PRICE'],
                product_csv['PROD_MIN_PRICE']):

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
            df_countries_ext = pd.DataFrame(products_dict)
            df_countries_ext.to_sql('products_ext', ses_db_stg, if_exists="append",index=False)
        ses_db_stg.dispose()
    except:
        traceback.print_exc()
    finally:
        pass