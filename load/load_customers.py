from logging import exception
from util.db_connection import Db_Connection
from transform.transformations import obt_gender
from util.configurationReader import readDataBaseConfigurations
from datetime import datetime
import pandas as pd
import traceback

def load_customers(etl_sync_id):
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

        country_sor = pd.read_sql(f"SELECT ID, COUNTRY_ID FROM countries_dim", ses_db_sor)
        #Dictionary for country key
        country_dict = dict()
        if not country_sor.empty:
            for id,cou_id \
                in zip(country_sor['ID'],country_sor['COUNTRY_ID']):
                country_dict[cou_id] = id
        
        #Dictionary for values of customers_tra
        customers_dict_tra = {
            "cust_id":[],
            "cust_first_name":[],
            "cust_last_name":[],
            "cust_gender":[],
            "cust_year_of_birth":[],
            "cust_marital_status":[],
            "cust_street_address":[],
            "cust_postal_code":[],
            "cust_city":[],
            "cust_state_province":[],
            "country_id":[],
            "cust_main_phone_integer":[],
            "cust_income_level":[],
            "cust_credit_limit":[],
            "cust_email":[]
        }
        #Dictionary for values of customers_dim
        customers_dict = {
            "cust_id":[],
            "cust_first_name":[],
            "cust_last_name":[],
            "cust_gender":[],
            "cust_year_of_birth":[],
            "cust_marital_status":[],
            "cust_street_address":[],
            "cust_postal_code":[],
            "cust_city":[],
            "cust_state_province":[],
            "country_id":[],
            "cust_main_phone_integer":[],
            "cust_income_level":[],
            "cust_credit_limit":[],
            "cust_email":[]
        }
        #Read tra table
        customer_tra = pd.read_sql(f"SELECT CUST_ID, CUST_FIRST_NAME, CUST_LAST_NAME, CUST_GENDER,CUST_YEAR_OF_BIRTH,CUST_MARITAL_STATUS,CUST_STREET_ADDRESS,CUST_POSTAL_CODE,CUST_CITY,CUST_STATE_PROVINCE,COUNTRY_ID,CUST_MAIN_PHONE_INTEGER,CUST_INCOME_LEVEL,CUST_CREDIT_LIMIT,CUST_EMAIL FROM customers_tra WHERE ETL_SYNC_ID = {etl_sync_id}", ses_db_stg)
        #Process Content
        if not customer_tra.empty:
            for id,fir_nam,las_nam,gen,yea_bir,mar_sta,str_add,pos_cod,cit,sta_pro,cou_id,mai_pho,inc_lev,cre_lim,ema \
                in zip(customer_tra['CUST_ID'],customer_tra['CUST_FIRST_NAME'],
                customer_tra['CUST_LAST_NAME'], customer_tra['CUST_GENDER'],
                customer_tra['CUST_YEAR_OF_BIRTH'],customer_tra['CUST_MARITAL_STATUS'],
                customer_tra['CUST_STREET_ADDRESS'],customer_tra['CUST_POSTAL_CODE'],
                customer_tra['CUST_CITY'],customer_tra['CUST_STATE_PROVINCE'],
                customer_tra['COUNTRY_ID'],customer_tra['CUST_MAIN_PHONE_INTEGER'],
                customer_tra['CUST_INCOME_LEVEL'],customer_tra['CUST_CREDIT_LIMIT'],
                customer_tra['CUST_EMAIL']):

                country_id = country_dict[cou_id]

                customers_dict_tra["cust_id"].append(id)
                customers_dict_tra["cust_first_name"].append(fir_nam)
                customers_dict_tra["cust_last_name"].append(las_nam)
                customers_dict_tra["cust_gender"].append(gen)
                customers_dict_tra["cust_year_of_birth"].append(yea_bir)
                customers_dict_tra["cust_marital_status"].append(mar_sta)
                customers_dict_tra["cust_street_address"].append(str_add)
                customers_dict_tra["cust_postal_code"].append(pos_cod)
                customers_dict_tra["cust_city"].append(cit)
                customers_dict_tra["cust_state_province"].append(sta_pro)
                customers_dict_tra["country_id"].append(country_id)
                customers_dict_tra["cust_main_phone_integer"].append(mai_pho)
                customers_dict_tra["cust_income_level"].append(inc_lev)
                customers_dict_tra["cust_credit_limit"].append(cre_lim)
                customers_dict_tra["cust_email"].append(ema)

        customer_sor = pd.read_sql(f"SELECT CUST_ID, CUST_FIRST_NAME, CUST_LAST_NAME, CUST_GENDER,CUST_YEAR_OF_BIRTH,CUST_MARITAL_STATUS,CUST_STREET_ADDRESS,CUST_POSTAL_CODE,CUST_CITY,CUST_STATE_PROVINCE,COUNTRY_ID,CUST_MAIN_PHONE_INTEGER,CUST_INCOME_LEVEL,CUST_CREDIT_LIMIT,CUST_EMAIL FROM customers_dim", ses_db_sor)
        if not customer_sor.empty:
            for id,fir_nam,las_nam,gen,yea_bir,mar_sta,str_add,pos_cod,cit,sta_pro,cou_id,mai_pho,inc_lev,cre_lim,ema \
                in zip(customer_sor['CUST_ID'],customer_sor['CUST_FIRST_NAME'],
                customer_sor['CUST_LAST_NAME'], customer_sor['CUST_GENDER'],
                customer_sor['CUST_YEAR_OF_BIRTH'],customer_sor['CUST_MARITAL_STATUS'],
                customer_sor['CUST_STREET_ADDRESS'],customer_sor['CUST_POSTAL_CODE'],
                customer_sor['CUST_CITY'],customer_sor['CUST_STATE_PROVINCE'],
                customer_sor['COUNTRY_ID'],customer_sor['CUST_MAIN_PHONE_INTEGER'],
                customer_sor['CUST_INCOME_LEVEL'],customer_sor['CUST_CREDIT_LIMIT'],
                customer_sor['CUST_EMAIL']):

                customers_dict["cust_id"].append(id)
                customers_dict["cust_first_name"].append(fir_nam)
                customers_dict["cust_last_name"].append(las_nam)
                customers_dict["cust_gender"].append(gen)
                customers_dict["cust_year_of_birth"].append(yea_bir)
                customers_dict["cust_marital_status"].append(mar_sta)
                customers_dict["cust_street_address"].append(str_add)
                customers_dict["cust_postal_code"].append(pos_cod)
                customers_dict["cust_city"].append(cit)
                customers_dict["cust_state_province"].append(sta_pro)
                customers_dict["country_id"].append(cou_id)
                customers_dict["cust_main_phone_integer"].append(mai_pho)
                customers_dict["cust_income_level"].append(inc_lev)
                customers_dict["cust_credit_limit"].append(cre_lim)
                customers_dict["cust_email"].append(ema)

        if customers_dict["cust_id"]:
            df_customers_tra = pd.DataFrame(customers_dict_tra)
            df_customers_sor = pd.DataFrame(customers_dict)
            df_customers_dim = df_customers_tra.merge(df_customers_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            df_customers_dim.to_sql('customers_dim', ses_db_sor, if_exists="append",index=False)
        else:
            df_customers_dim = pd.DataFrame(customers_dict_tra)
            df_customers_dim.to_sql('customers_dim', ses_db_sor, if_exists="append",index=False)
        
        ses_db_stg.dispose()
        ses_db_sor.dispose()
    except:
        traceback.print_exc()
    finally:
        pass