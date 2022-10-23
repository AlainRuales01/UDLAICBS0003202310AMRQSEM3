from logging import exception
from util.db_connection import Db_Connection
from transform.transformations import obt_gender
from util.configurationReader import readDataBaseConfigurations
from datetime import datetime
import pandas as pd
import traceback

def tra_customers(etl_sync_id):
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

        #Dictionary for values of customers_tra
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
            "cust_email":[],
            "etl_sync_id":[]
        }

        customer_ext = pd.read_sql("SELECT CUST_ID, CUST_FIRST_NAME, CUST_LAST_NAME, CUST_GENDER,CUST_YEAR_OF_BIRTH,CUST_MARITAL_STATUS,CUST_STREET_ADDRESS,CUST_POSTAL_CODE,CUST_CITY,CUST_STATE_PROVINCE,COUNTRY_ID,CUST_MAIN_PHONE_INTEGER,CUST_INCOME_LEVEL,CUST_CREDIT_LIMIT,CUST_EMAIL FROM customers_ext", ses_db_stg)
        #Process CSV Content
        if not customer_ext.empty:
            for id,fir_nam,las_nam,gen,yea_bir,mar_sta,str_add,pos_cod,cit,sta_pro,cou_id,mai_pho,inc_lev,cre_lim,ema \
                in zip(customer_ext['CUST_ID'],customer_ext['CUST_FIRST_NAME'],
                customer_ext['CUST_LAST_NAME'], customer_ext['CUST_GENDER'],
                customer_ext['CUST_YEAR_OF_BIRTH'],customer_ext['CUST_MARITAL_STATUS'],
                customer_ext['CUST_STREET_ADDRESS'],customer_ext['CUST_POSTAL_CODE'],
                customer_ext['CUST_CITY'],customer_ext['CUST_STATE_PROVINCE'],
                customer_ext['COUNTRY_ID'],customer_ext['CUST_MAIN_PHONE_INTEGER'],
                customer_ext['CUST_INCOME_LEVEL'],customer_ext['CUST_CREDIT_LIMIT'],
                customer_ext['CUST_EMAIL']):

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
                customers_dict["etl_sync_id"].append(etl_sync_id)


        if customers_dict["cust_id"]:
            df_customers_tra = pd.DataFrame(customers_dict)
            df_customers_tra.to_sql('customers_tra', ses_db_stg, if_exists="append",index=False)
        ses_db_stg.dispose()
    except:
        traceback.print_exc()
    finally:
        pass