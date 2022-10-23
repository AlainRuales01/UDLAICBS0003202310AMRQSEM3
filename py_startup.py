from logging import exception
from util.deleteDataBaseData import deleteData
from util.insert_ETLSync import insert_ETLSync
from util.extract_etl import execute_extract
from util.transform_etl import execute_transform
from datetime import datetime
import traceback





try:
    # deleteData()
    etl_process = insert_ETLSync()
    #execute_extract()
    execute_transform(etl_process)
except:
    traceback.print_exc()
finally:
    pass