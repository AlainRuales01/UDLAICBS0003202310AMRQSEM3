from logging import exception
from util.db_connection import Db_Connection
from util.configurationReader import readDataBaseConfigurations
from datetime import datetime
import pandas as pd
import traceback

def load_countries(etl_sync_id):
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
        
        
        #Dictionary for values of countries_tra
        countries_dict_tra = {
            "country_id":[],
            "country_name":[],
            "country_region":[],
            "country_region_id":[],
        }

        #Dictionary for values of countries_dim
        countries_dict = {
            "country_id":[],
            "country_name":[],
            "country_region":[],
            "country_region_id":[],
        }
        #Read tra table
        country_tra = pd.read_sql(f"SELECT COUNTRY_ID, COUNTRY_NAME, COUNTRY_REGION, COUNTRY_REGION_ID FROM countries_tra WHERE ETL_SYNC_ID = {etl_sync_id}", ses_db_stg)
        #Process CSV Content
        if not country_tra.empty:
            for id,nam,reg,reg_id \
                in zip(country_tra['COUNTRY_ID'],country_tra['COUNTRY_NAME'],
                country_tra['COUNTRY_REGION'], country_tra['COUNTRY_REGION_ID']):
                countries_dict_tra["country_id"].append(id)
                countries_dict_tra["country_name"].append(nam)
                countries_dict_tra["country_region"].append(reg)
                countries_dict_tra["country_region_id"].append(reg_id)

        #Read tra table
        country_sor = pd.read_sql("SELECT COUNTRY_ID, COUNTRY_NAME, COUNTRY_REGION, COUNTRY_REGION_ID FROM countries_dim", ses_db_sor)
        if not country_sor.empty:
            for id,nam,reg,reg_id \
                in zip(country_sor['COUNTRY_ID'],country_sor['COUNTRY_NAME'],
                country_sor['COUNTRY_REGION'], country_sor['COUNTRY_REGION_ID']):
                countries_dict["country_id"].append(id)
                countries_dict["country_name"].append(nam)
                countries_dict["country_region"].append(reg)
                countries_dict["country_region_id"].append(reg_id)
        
        if countries_dict["country_id"]:
            df_countries_tra = pd.DataFrame(countries_dict_tra)
            df_countries_sor = pd.DataFrame(countries_dict)
            df_countries_dim = df_countries_tra.merge(df_countries_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            df_countries_dim.to_sql('countries_dim', ses_db_sor, if_exists="append",index=False)
        else:
            df_countries_dim = pd.DataFrame(countries_dict_tra)
            df_countries_dim.to_sql('countries_dim', ses_db_sor, if_exists="append",index=False)
        
        ses_db_stg.dispose()
        ses_db_sor.dispose()
    except:
        traceback.print_exc()
    finally:
        pass