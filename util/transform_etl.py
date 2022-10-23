from transform.transform_channels import tra_channels
from transform.transform_countries import tra_countries
from transform.transform_customers import tra_customers
from transform.transform_products import tra_products
from transform.transform_promotions import tra_promotions
from transform.transform_sales import tra_sales
from transform.transform_times import tra_times

def execute_transform(etl_process):
    tra_channels(etl_process)
    tra_countries(etl_process)
    tra_customers(etl_process)
    tra_products(etl_process)
    tra_promotions(etl_process)
    tra_times(etl_process)
    tra_sales(etl_process)
    
