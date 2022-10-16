from logging import exception
from util.db_connection import Db_Connection
from datetime import datetime
import pandas as pd
import traceback

def ext_customers():
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

        customer_csv = pd.read_csv("CSV/customers.csv")
        #Process CSV Content
        if not customer_csv.empty:
            for id,fir_nam,las_nam,gen,yea_bir,mar_sta,str_add,pos_cod,cit,sta_pro,cou_id,mai_pho,inc_lev,cre_lim,ema \
                in zip(customer_csv['CUST_ID'],customer_csv['CUST_FIRST_NAME'],
                customer_csv['CUST_LAST_NAME'], customer_csv['CUST_GENDER'],
                customer_csv['CUST_YEAR_OF_BIRTH'],customer_csv['CUST_MARITAL_STATUS'],
                customer_csv['CUST_STREET_ADDRESS'],customer_csv['CUST_POSTAL_CODE'],
                customer_csv['CUST_CITY'],customer_csv['CUST_STATE_PROVINCE'],
                customer_csv['COUNTRY_ID'],customer_csv['CUST_MAIN_PHONE_NUMBER'],
                customer_csv['CUST_INCOME_LEVEL'],customer_csv['CUST_CREDIT_LIMIT'],
                customer_csv['CUST_EMAIL']):

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
            # ses_db_stg.connect().execute("TRUNCATE TABLE channels_ext")
            df_countries_ext = pd.DataFrame(customers_dict)
            df_countries_ext.to_sql('customers_ext', ses_db_stg, if_exists="append",index=False)
    except:
        traceback.print_exc()
    finally:
        pass