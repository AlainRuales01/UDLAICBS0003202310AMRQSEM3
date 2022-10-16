from logging import exception
from util.db_connection import Db_Connection
from datetime import datetime
from extract.extract_channels import ext_channels
from extract.extract_countries import ext_countries
from extract.extract_customers import ext_customers
from extract.extract_products import ext_products
from extract.extract_promotions import ext_promotions
from extract.extract_sales import ext_sales
from extract.extract_times import ext_times
from util.deleteDataBaseData import deleteData
from util import configurationReader
import pandas as pd
import traceback

def executeExtract():
    ext_countries()
    ext_promotions()
    ext_times()
    ext_channels()
    ext_products()
    ext_customers()
    ext_sales()

try:
    deleteData()
    executeExtract()
except:
    traceback.print_exc()
finally:
    pass