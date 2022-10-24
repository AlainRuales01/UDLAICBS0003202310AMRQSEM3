use amrqdbstg;

select * from ETLSync;

select * from channels_ext;
select * from countries_ext;
select  count(*) from customers_ext;
select * from products_ext;
select count(*) from promotions_ext;
select count(*) from sales_ext;
select count(*) from times_ext;



select * from channels_tra where ETL_SYNC_ID = 59;
select * from countries_tra where ETL_SYNC_ID = 58;
select * from customers_tra where ETL_SYNC_ID = 58;
select * from products_tra where ETL_SYNC_ID = 58;
select * from promotions_tra where ETL_SYNC_ID = 58; 
select * from sales_tra where ETL_SYNC_ID = 58;
select * from times_tra where ETL_SYNC_ID = 58;