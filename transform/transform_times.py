from logging import exception
from util.db_connection import Db_Connection
from util.configurationReader import readDataBaseConfigurations
from datetime import datetime
from transform.transformations import obt_date
import pandas as pd
import traceback

def tra_times(etl_sync_id):
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

        #Dictionary for values of times_tra
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
            "calendar_year":[],
            "etl_sync_id":[]
        }

        time_ext = pd.read_sql("SELECT TIME_ID, DAY_NAME, DAY_INTEGER_IN_WEEK, DAY_INTEGER_IN_MONTH,CALENDAR_WEEK_INTEGER,CALENDAR_MONTH_INTEGER,CALENDAR_MONTH_DESC,END_OF_CAL_MONTH,CALENDAR_QUARTER_DESC,CALENDAR_YEAR FROM times_ext", ses_db_stg)
        #Process CSV Content
        if not time_ext.empty:
            for id,day_nam,day_int_wee,day_int_mon,cal_wee_int,cal_mon_int,cal_mon_des,end_cal_mon,cal_qua_des,cal_yea \
                in zip(time_ext['TIME_ID'],time_ext['DAY_NAME'],
                time_ext['DAY_INTEGER_IN_WEEK'], time_ext['DAY_INTEGER_IN_MONTH'],
                time_ext['CALENDAR_WEEK_INTEGER'],time_ext['CALENDAR_MONTH_INTEGER'],
                time_ext['CALENDAR_MONTH_DESC'],time_ext['END_OF_CAL_MONTH'],
                time_ext['CALENDAR_QUARTER_DESC'],time_ext['CALENDAR_YEAR']):

                times_dict["time_id"].append(obt_date(id))
                times_dict["day_name"].append(day_nam)
                times_dict["day_integer_in_week"].append(day_int_wee)
                times_dict["day_integer_in_month"].append(day_int_mon)
                times_dict["calendar_week_integer"].append(cal_wee_int)
                times_dict["calendar_month_integer"].append(cal_mon_int)
                times_dict["calendar_month_desc"].append(cal_mon_des)
                times_dict["end_of_cal_month"].append(obt_date(end_cal_mon))
                times_dict["calendar_quarter_desc"].append(cal_qua_des)
                times_dict["calendar_year"].append(cal_yea)
                times_dict["calendar_month_name"].append("")
                times_dict["etl_sync_id"].append(etl_sync_id)


        if times_dict["time_id"]:
            df_times_tra = pd.DataFrame(times_dict)
            df_times_tra.to_sql('times_tra', ses_db_stg, if_exists="append",index=False)
            
        ses_db_stg.dispose()
    except:
        traceback.print_exc()
    finally:
        pass