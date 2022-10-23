from logging import exception
from util.db_connection import Db_Connection
from util.configurationReader import readDataBaseConfigurations
from datetime import datetime
import pandas as pd
import traceback

def tra_countries(etl_sync_id):
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

        #Dictionary for values of countries_tra
        countries_dict = {
            "country_id":[],
            "country_name":[],
            "country_region":[],
            "country_region_id":[],
            "etl_sync_id":[]
        }

        country_ext = pd.read_sql("SELECT COUNTRY_ID, COUNTRY_NAME, COUNTRY_REGION, COUNTRY_REGION_ID FROM countries_ext", ses_db_stg)
        #Process CSV Content
        if not country_ext.empty:
            for id,nam,reg,reg_id \
                in zip(country_ext['COUNTRY_ID'],country_ext['COUNTRY_NAME'],
                country_ext['COUNTRY_REGION'], country_ext['COUNTRY_REGION_ID']):
                countries_dict["country_id"].append(id)
                countries_dict["country_name"].append(nam)
                countries_dict["country_region"].append(reg)
                countries_dict["country_region_id"].append(reg_id)
                countries_dict["etl_sync_id"].append(etl_sync_id)
        if countries_dict["country_id"]:
            df_countries_tra = pd.DataFrame(countries_dict)
            df_countries_tra.to_sql('countries_tra', ses_db_stg, if_exists="append",index=False)
        ses_db_stg.dispose()
    except:
        traceback.print_exc()
    finally:
        pass