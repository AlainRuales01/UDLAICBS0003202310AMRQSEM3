from logging import exception
from util.db_connection import Db_Connection
from util.configurationReader import readDataBaseConfigurations
from datetime import datetime
from transform.transformations import obt_date
import pandas as pd
import traceback

def load_times(etl_sync_id):
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
        
        #Dictionary for values of times_tra
        times_dict_tra = {
            "time_id":[],
            "day_name":[],
            "day_integer_in_week":[],
            "day_integer_in_month":[],
            "calendar_week_integer":[],
            "calendar_month_integer":[],
            "calendar_month_desc":[],
            "end_of_cal_month":[],
            "calendar_month_name":[],
            "calendar_quarter_desc":[],
            "calendar_year":[]
        }

        times_dict = {
            "time_id":[],
            "day_name":[],
            "day_integer_in_week":[],
            "day_integer_in_month":[],
            "calendar_week_integer":[],
            "calendar_month_integer":[],
            "calendar_month_desc":[],
            "end_of_cal_month":[],
            "calendar_month_name":[],
            "calendar_quarter_desc":[],
            "calendar_year":[]
        }

        time_tra = pd.read_sql(f"SELECT TIME_ID, DAY_NAME, DAY_INTEGER_IN_WEEK, DAY_INTEGER_IN_MONTH,CALENDAR_WEEK_INTEGER,CALENDAR_MONTH_INTEGER,CALENDAR_MONTH_DESC,END_OF_CAL_MONTH,CALENDAR_QUARTER_DESC,CALENDAR_YEAR FROM times_tra WHERE ETL_SYNC_ID = {etl_sync_id}", ses_db_stg)
        #Process Content
        if not time_tra.empty:
            for id,day_nam,day_int_wee,day_int_mon,cal_wee_int,cal_mon_int,cal_mon_des,end_cal_mon,cal_qua_des,cal_yea \
                in zip(time_tra['TIME_ID'],time_tra['DAY_NAME'],
                time_tra['DAY_INTEGER_IN_WEEK'], time_tra['DAY_INTEGER_IN_MONTH'],
                time_tra['CALENDAR_WEEK_INTEGER'],time_tra['CALENDAR_MONTH_INTEGER'],
                time_tra['CALENDAR_MONTH_DESC'],time_tra['END_OF_CAL_MONTH'],
                time_tra['CALENDAR_QUARTER_DESC'],time_tra['CALENDAR_YEAR']):

                times_dict_tra["time_id"].append(id)
                times_dict_tra["day_name"].append(day_nam)
                times_dict_tra["day_integer_in_week"].append(day_int_wee)
                times_dict_tra["day_integer_in_month"].append(day_int_mon)
                times_dict_tra["calendar_week_integer"].append(cal_wee_int)
                times_dict_tra["calendar_month_integer"].append(cal_mon_int)
                times_dict_tra["calendar_month_desc"].append(cal_mon_des)
                times_dict_tra["end_of_cal_month"].append(end_cal_mon)
                times_dict_tra["calendar_quarter_desc"].append(cal_qua_des)
                times_dict_tra["calendar_year"].append(cal_yea)
                times_dict_tra["calendar_month_name"].append("")
        
        time_sor = pd.read_sql(f"SELECT TIME_ID, DAY_NAME, DAY_INTEGER_IN_WEEK, DAY_INTEGER_IN_MONTH,CALENDAR_WEEK_INTEGER,CALENDAR_MONTH_INTEGER,CALENDAR_MONTH_DESC,END_OF_CAL_MONTH,CALENDAR_QUARTER_DESC,CALENDAR_YEAR FROM times_dim", ses_db_sor)
        if not time_sor.empty:
            for id,day_nam,day_int_wee,day_int_mon,cal_wee_int,cal_mon_int,cal_mon_des,end_cal_mon,cal_qua_des,cal_yea \
                in zip(time_sor['TIME_ID'],time_sor['DAY_NAME'],
                time_sor['DAY_INTEGER_IN_WEEK'], time_sor['DAY_INTEGER_IN_MONTH'],
                time_sor['CALENDAR_WEEK_INTEGER'],time_sor['CALENDAR_MONTH_INTEGER'],
                time_sor['CALENDAR_MONTH_DESC'],time_sor['END_OF_CAL_MONTH'],
                time_sor['CALENDAR_QUARTER_DESC'],time_sor['CALENDAR_YEAR']):

                times_dict["time_id"].append(id)
                times_dict["day_name"].append(day_nam)
                times_dict["day_integer_in_week"].append(day_int_wee)
                times_dict["day_integer_in_month"].append(day_int_mon)
                times_dict["calendar_week_integer"].append(cal_wee_int)
                times_dict["calendar_month_integer"].append(cal_mon_int)
                times_dict["calendar_month_desc"].append(cal_mon_des)
                times_dict["end_of_cal_month"].append(end_cal_mon)
                times_dict["calendar_quarter_desc"].append(cal_qua_des)
                times_dict["calendar_year"].append(cal_yea)
                times_dict["calendar_month_name"].append("")
        
        if times_dict["time_id"]:
            df_times_tra = pd.DataFrame(times_dict_tra)
            df_times_sor = pd.DataFrame(times_dict)
            df_times_dim = df_times_tra.merge(df_times_sor, indicator='i', how='outer').query('i == "left_only"').drop('i', axis=1)
            df_times_dim.to_sql('times_dim', ses_db_sor, if_exists="append",index=False)
        else:
            df_times_dim = pd.DataFrame(times_dict_tra)
            df_times_dim.to_sql('times_dim', ses_db_sor, if_exists="append",index=False)
        
        ses_db_stg.dispose()
        ses_db_sor.dispose()
    except:
        traceback.print_exc()
    finally:
        pass