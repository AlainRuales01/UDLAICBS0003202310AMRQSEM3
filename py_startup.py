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
import pandas as pd
import traceback


try:
    # ext_channels()
    # ext_countries()
    # ext_customers()
    # ext_products()
    # ext_promotions()
    ext_sales()
    # ext_times()

except:
    traceback.print_exc()
finally:
    pass