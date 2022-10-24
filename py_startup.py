from logging import exception
from util.etl_process.deleteDataBaseData import deleteData
from util.etl_process.insert_ETLSync import insert_ETLSync
from util.etl_process.extract_etl import execute_extract
from util.etl_process.transform_etl import execute_transform
from util.etl_process.load_etl import execute_load
from datetime import datetime
import traceback





try:
    deleteData()
    etl_process = insert_ETLSync()
    execute_extract()
    execute_transform(etl_process)
    execute_load(etl_process)


except:
    traceback.print_exc()
finally:
    pass