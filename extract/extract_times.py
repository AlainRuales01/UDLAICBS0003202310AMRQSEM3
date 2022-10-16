from logging import exception
from util.db_connection import Db_Connection
from util.configurationReader import readDataBaseConfigurations
from util.configurationReader import readRouteConfigurations
from datetime import datetime
import pandas as pd
import traceback

def ext_times():
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

        time_csv = pd.read_csv("{}times.csv".format(routeConfiguration["CSV_ROUTE"]))
        #Process CSV Content
        if not time_csv.empty:
            for id,day_nam,day_int_wee,day_int_mon,cal_wee_int,cal_mon_int,cal_mon_des,end_cal_mon,cal_qua_des,cal_yea \
                in zip(time_csv['TIME_ID'],time_csv['DAY_NAME'],
                time_csv['DAY_NUMBER_IN_WEEK'], time_csv['DAY_NUMBER_IN_MONTH'],
                time_csv['CALENDAR_WEEK_NUMBER'],time_csv['CALENDAR_MONTH_NUMBER'],
                time_csv['CALENDAR_MONTH_DESC'],time_csv['END_OF_CAL_MONTH'],
                time_csv['CALENDAR_QUARTER_DESC'],time_csv['CALENDAR_YEAR']):

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
            df_countries_ext = pd.DataFrame(times_dict)
            df_countries_ext.to_sql('times_ext', ses_db_stg, if_exists="append",index=False)
            
        ses_db_stg.dispose()
    except:
        traceback.print_exc()
    finally:
        pass