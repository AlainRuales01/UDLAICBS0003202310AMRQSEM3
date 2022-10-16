from logging import exception
from util.db_connection import Db_Connection
from util.configurationReader import readDataBaseConfigurations
from util.configurationReader import readRouteConfigurations
from datetime import datetime
import pandas as pd
import traceback

def ext_countries():
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
        countries_dict = {
            "country_id":[],
            "country_name":[],
            "country_region":[],
            "country_region_id":[]
        }

        country_csv = pd.read_csv("{}countries.csv".format(routeConfiguration["CSV_ROUTE"]))
        #Process CSV Content
        if not country_csv.empty:
            for id,nam,reg,reg_id \
                in zip(country_csv['COUNTRY_ID'],country_csv['COUNTRY_NAME'],
                country_csv['COUNTRY_REGION'], country_csv['COUNTRY_REGION_ID']):
                countries_dict["country_id"].append(id)
                countries_dict["country_name"].append(nam)
                countries_dict["country_region"].append(reg)
                countries_dict["country_region_id"].append(reg_id)
        if countries_dict["country_id"]:
            df_countries_ext = pd.DataFrame(countries_dict)
            df_countries_ext.to_sql('countries_ext', ses_db_stg, if_exists="append",index=False)
        ses_db_stg.dispose()
    except:
        traceback.print_exc()
    finally:
        pass